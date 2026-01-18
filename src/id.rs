//! Centralized ID generation for resources.
//!
//! Format: [4-char prefix][26-char nanoid] = 30 chars total
//! Alphabet: lowercase alphanumeric (0-9, a-z) for k8s compatibility

/// Custom lowercase alphabet for k8s compatibility
const ID_ALPHABET: [char; 36] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Macro that defines the ResourceId enum and validates at compile time:
/// - All prefixes are unique
/// - All prefixes are exactly 4 characters
/// - All prefix characters are in the ID alphabet (0-9, a-z)
///
/// # Compile-time validation
///
/// Duplicate prefixes fail to compile:
/// ```compile_fail
/// runtimedb::define_resource_ids! {
///     Foo => "test",
///     Bar => "test",
/// }
/// ```
///
/// Prefix must be exactly 4 characters:
/// ```compile_fail
/// runtimedb::define_resource_ids! {
///     Foo => "ab",
/// }
/// ```
///
/// ```compile_fail
/// runtimedb::define_resource_ids! {
///     Foo => "abcde",
/// }
/// ```
///
/// Prefix must be lowercase alphanumeric (0-9, a-z):
/// ```compile_fail
/// runtimedb::define_resource_ids! {
///     Foo => "CONN",
/// }
/// ```
///
/// ```compile_fail
/// runtimedb::define_resource_ids! {
///     Foo => "ab-c",
/// }
/// ```
#[macro_export]
macro_rules! define_resource_ids {
    ($($variant:ident => $prefix:literal),+ $(,)?) => {
        const _: () = {
            const PREFIXES: &[&str] = &[$($prefix),+];

            const fn is_valid_char(c: u8) -> bool {
                (c >= b'0' && c <= b'9') || (c >= b'a' && c <= b'z')
            }

            const fn validate_prefixes() {
                let mut i = 0;
                while i < PREFIXES.len() {
                    let prefix = PREFIXES[i].as_bytes();

                    // Check length is exactly 4
                    if prefix.len() != 4 {
                        panic!("prefix must be exactly 4 characters");
                    }

                    // Check all characters are in alphabet (0-9, a-z)
                    let mut c = 0;
                    while c < prefix.len() {
                        if !is_valid_char(prefix[c]) {
                            panic!("prefix must only contain lowercase alphanumeric (0-9, a-z)");
                        }
                        c += 1;
                    }

                    // Check uniqueness against subsequent prefixes
                    let mut j = i + 1;
                    while j < PREFIXES.len() {
                        let other = PREFIXES[j].as_bytes();
                        let mut k = 0;
                        let mut equal = true;
                        while k < 4 {
                            if prefix[k] != other[k] { equal = false; }
                            k += 1;
                        }
                        if equal { panic!("duplicate prefix"); }
                        j += 1;
                    }
                    i += 1;
                }
            }
            validate_prefixes();
        };

        /// The type of resource ID, determining its prefix.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum ResourceId {
            $($variant),+
        }

        impl ResourceId {
            /// Returns the 4-character prefix for this resource ID type.
            pub const fn prefix(&self) -> &'static str {
                match self {
                    $(Self::$variant => $prefix),+
                }
            }
        }
    };
}

define_resource_ids! {
    Connection => "conn",
    Result => "rslt",
}

/// Generate a 30-char ID: 4-char prefix + 26-char nanoid (lowercase alphanumeric).
pub fn generate_id(resource: ResourceId) -> String {
    let suffix = nanoid::nanoid!(26, &ID_ALPHABET);
    format!("{}{}", resource.prefix(), suffix)
}

/// Generate a connection ID (prefix: "conn").
pub fn generate_connection_id() -> String {
    generate_id(ResourceId::Connection)
}

/// Generate a result ID (prefix: "rslt").
pub fn generate_result_id() -> String {
    generate_id(ResourceId::Result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_id_format() {
        let id = generate_connection_id();
        assert_eq!(id.len(), 30);
        assert!(id.starts_with("conn"));
        assert!(id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn test_result_id_format() {
        let id = generate_result_id();
        assert_eq!(id.len(), 30);
        assert!(id.starts_with("rslt"));
        assert!(id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn test_ids_are_unique() {
        let id1 = generate_connection_id();
        let id2 = generate_connection_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_resource_id_prefixes() {
        assert_eq!(ResourceId::Connection.prefix(), "conn");
        assert_eq!(ResourceId::Result.prefix(), "rslt");
    }
}
