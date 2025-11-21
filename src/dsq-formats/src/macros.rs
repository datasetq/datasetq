/// Macros to reduce boilerplate for format implementations

/// Macro to generate convenience functions for a format
#[macro_export]
macro_rules! format_convenience_fns {
    (
        $name:ident,
        $reader_struct:ty,
        $writer_struct:ty,
        $read_options:ty,
        $write_options:ty,
        $detect_fn:ident
    ) => {
        paste::paste! {
            /// Convenience function to read $name from a file path
            pub fn [<read_ $name _file>]<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
                let file = File::open(path)?;
                let mut reader = <$reader_struct>::new(file);
                reader.read_dataframe()
            }

            /// Convenience function to read $name from a file path with options
            pub fn [<read_ $name _file_with_options>]<P: AsRef<Path>>(path: P, options: &$read_options) -> Result<DataFrame> {
                let file = File::open(path)?;
                let mut reader = <$reader_struct>::with_options(file, options.clone());
                reader.read_dataframe()
            }

            /// Convenience function to write DataFrame to $name file
            pub fn [<write_ $name _file>]<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
                let file = File::create(path)?;
                let mut writer = <$writer_struct>::new(file);
                writer.write_dataframe(df)
            }

            /// Convenience function to write DataFrame to $name file with options
            pub fn [<write_ $name _file_with_options>]<P: AsRef<Path>>(df: &DataFrame, path: P, options: &$write_options) -> Result<()> {
                let file = File::create(path)?;
                let mut writer = <$writer_struct>::with_options(file, options.clone());
                writer.write_dataframe(df)
            }

            /// Detect if content is in $name format
            pub fn [<detect_ $name _format>](bytes: &[u8]) -> bool {
                $detect_fn(bytes)
            }
        }
    };
}

/// Macro to generate format-specific read options with common fields
#[macro_export]
macro_rules! format_read_options {
    ($name:ident { $($field:ident: $type:ty = $default:expr),* $(,)? }) => {
        /// Read options for $name format
        #[derive(Debug, Clone)]
        pub struct $name {
            $(
                /// Documentation for $field
                pub $field: $type,
            )*
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $(
                        $field: $default,
                    )*
                }
            }
        }
    };
}

/// Macro to generate format-specific write options with common fields
#[macro_export]
macro_rules! format_write_options {
    ($name:ident { $($field:ident: $type:ty = $default:expr),* $(,)? }) => {
        /// Write options for $name format
        #[derive(Debug, Clone)]
        pub struct $name {
            $(
                /// Documentation for $field
                pub $field: $type,
            )*
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $(
                        $field: $default,
                    )*
                }
            }
        }


    };
}

/// Macro to implement common format detection functions
#[macro_export]
macro_rules! impl_format_detection {
    ($format:expr, $detect_fn:ident, $magic_bytes:expr) => {
        /// Detect if content starts with magic bytes for this format
        pub fn $detect_fn(bytes: &[u8]) -> bool {
            if $magic_bytes.is_empty() {
                false
            } else {
                bytes.len() >= $magic_bytes.len() && &bytes[0..$magic_bytes.len()] == $magic_bytes
            }
        }
    };

    ($format:expr, $detect_fn:ident, content_detection) => {
        /// Detect format from content analysis
        pub fn $detect_fn(bytes: &[u8]) -> bool {
            // Basic content-based detection: check for JSON-like structure
            if bytes.is_empty() {
                false
            } else {
                let s = std::str::from_utf8(bytes).unwrap_or("");
                s.trim_start().starts_with('{') || s.trim_start().starts_with('[')
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test the impl_format_detection macro with magic bytes
    impl_format_detection!("test", detect_test_format, b"TEST");

    #[test]
    fn test_detect_magic_bytes() {
        assert!(detect_test_format(b"TEST"));
        assert!(detect_test_format(b"TEST123"));
        assert!(!detect_test_format(b"TES"));
        assert!(!detect_test_format(b"test"));
        assert!(!detect_test_format(b""));
        assert!(!detect_test_format(&[]));
    }

    // Test the impl_format_detection macro with content detection
    impl_format_detection!("json", detect_json_format, content_detection);

    #[test]
    fn test_detect_content() {
        assert!(detect_json_format(b" { \"key\": \"value\" } "));
        assert!(detect_json_format(b"[1, 2, 3]"));
        assert!(!detect_json_format(b"not json"));
        assert!(!detect_json_format(b""));
        assert!(!detect_json_format(b"   "));
    }

    // Test format_read_options macro
    format_read_options! {
        TestReadOptions {
            separator: u8 = b',',
            has_header: bool = true
        }
    }

    #[test]
    fn test_read_options_default() {
        let opts = TestReadOptions::default();
        assert_eq!(opts.separator, b',');
        assert_eq!(opts.has_header, true);
    }

    // Test format_write_options macro
    format_write_options! {
        TestWriteOptions {
            separator: u8 = b';',
            quote_char: u8 = b'"'
        }
    }

    #[test]
    fn test_write_options_default() {
        let opts = TestWriteOptions::default();
        assert_eq!(opts.separator, b';');
        assert_eq!(opts.quote_char, b'"');
    }
}
