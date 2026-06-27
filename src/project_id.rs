//! First-class Projects routing.
//!
//! When a `project_id` is configured on the [`Client`](crate::Client), every
//! outbound request carries an `X-Raindrop-Project-Id: <slug>` header so the
//! backend files telemetry under the named project. When unset, no
//! header is sent and the backend falls back to the org's `default` project, so
//! existing callers stay byte-identical on the wire.
//!
//! This mirrors the `project_id` support in the Python and JS SDKs: the same
//! header name and the same slug grammar. Validation is fail-safe: an invalid
//! slug is logged once and dropped rather than breaking ingestion.

/// Header carrying the destination project slug.
pub(crate) const PROJECT_ID_HEADER: &str = "X-Raindrop-Project-Id";

/// Human-readable form of the accepted slug grammar, surfaced in the warning we
/// log for a rejected value. Kept in sync with [`is_valid_slug`].
pub(crate) const PROJECT_ID_SLUG_PATTERN: &str = "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$";

/// Maximum slug length permitted by the grammar (1 lead + up to 61 interior + 1
/// trailing character).
const MAX_SLUG_LEN: usize = 63;

/// Returns `true` when `value` matches the project-slug grammar
/// `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`.
///
/// Implemented as a manual scan rather than pulling in a regex engine, keeping
/// the crate's dependency surface small. Every accepted byte is ASCII, so
/// scanning bytes is equivalent to scanning characters here.
pub(crate) fn is_valid_slug(value: &str) -> bool {
    let bytes = value.as_bytes();
    let len = bytes.len();
    if len == 0 || len > MAX_SLUG_LEN {
        return false;
    }
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    // First and last characters must be lowercase-alphanumeric; a single-char
    // slug is just that one character.
    if !is_alnum(bytes[0]) || !is_alnum(bytes[len - 1]) {
        return false;
    }
    // Interior characters (strictly between the first and last) may also be
    // hyphens. For length 1 or 2 there is no interior and this slice is empty.
    bytes
        .get(1..len - 1)
        .is_none_or(|interior| interior.iter().all(|&b| is_alnum(b) || b == b'-'))
}

/// Normalize a caller-supplied project id into the slug to send on the wire, or
/// `None` when no header should be attached.
///
/// - `None`, empty, or whitespace-only -> `None` (omit the header; the backend
///   uses the org's default project).
/// - A valid slug, after trimming -> `Some(slug)`.
/// - A non-empty but invalid slug -> log a warning and return `None`, so a typo
///   never silently breaks ingestion.
pub(crate) fn resolve(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !is_valid_slug(trimmed) {
        tracing::warn!(
            project_id = trimmed,
            pattern = PROJECT_ID_SLUG_PATTERN,
            "raindrop: ignoring invalid project_id; it must match the slug pattern. \
             No X-Raindrop-Project-Id header will be sent."
        );
        return None;
    }
    Some(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_representative_valid_slugs() {
        for slug in [
            "a",
            "0",
            "ab",
            "a1",
            "my-project",
            "team-42",
            "a-b-c",
            "a--b",
            "project123",
            "0abc9",
            &"a".repeat(63),
        ] {
            assert!(is_valid_slug(slug), "expected {slug:?} to be valid");
        }
    }

    #[test]
    fn rejects_representative_invalid_slugs() {
        for slug in [
            "",              // empty
            "-abc",          // leading hyphen
            "abc-",          // trailing hyphen
            "-",             // hyphen only
            "Abc",           // uppercase
            "ABC",           // uppercase
            "my_project",    // underscore
            "my project",    // space
            "café",          // non-ascii
            "a.b",           // dot
            "a/b",           // slash
            &"a".repeat(64), // one over the length limit
        ] {
            assert!(!is_valid_slug(slug), "expected {slug:?} to be invalid");
        }
    }

    #[test]
    fn resolve_none_and_blank_return_none() {
        assert_eq!(resolve(None), None);
        assert_eq!(resolve(Some("")), None);
        assert_eq!(resolve(Some("   ")), None);
        assert_eq!(resolve(Some("\t\n")), None);
    }

    #[test]
    fn resolve_trims_surrounding_whitespace_before_validating() {
        assert_eq!(
            resolve(Some("  my-project  ")),
            Some("my-project".to_string())
        );
        assert_eq!(resolve(Some("\tteam-42\n")), Some("team-42".to_string()));
    }

    #[test]
    fn resolve_invalid_slug_is_dropped() {
        // Invalid values fail safe: header omitted (None) rather than raised.
        assert_eq!(resolve(Some("Invalid_Slug")), None);
        assert_eq!(resolve(Some("-leading")), None);
        assert_eq!(resolve(Some(&"a".repeat(64))), None);
    }
}
