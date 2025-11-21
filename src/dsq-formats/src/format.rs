use crate::error::{Error, FormatError, Result};
use std::path::Path;
use std::str::FromStr;

/// Supported data formats for reading and writing
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum DataFormat {
    /// Comma-separated values
    Csv,
    /// Tab-separated values
    Tsv,
    /// ASCII Delimited Text (using ASCII control characters 28-31)
    #[cfg_attr(feature = "cli", value(name = "adt", alias = "ascii-delimited"))]
    Adt,
    /// Apache Parquet columnar format
    Parquet,
    /// Apache Avro row-based format
    Avro,
    /// JSON Lines (newline-delimited JSON)
    #[cfg_attr(
        feature = "cli",
        value(name = "json-lines", alias = "jsonl", alias = "ndjson")
    )]
    JsonLines,
    /// Apache Arrow format
    Arrow,
    /// Standard JSON (array of objects)
    Json,
    /// Compact JSON (no pretty printing)
    #[cfg_attr(feature = "cli", value(name = "jsonc", alias = "json-compact"))]
    JsonCompact,
    /// JSON5 format (JSON with comments and relaxed syntax)
    Json5,
    /// Microsoft Excel format (output only)
    Excel,
    /// Apache ORC columnar format (output only)
    Orc,
}

impl DataFormat {
    /// Detect format from file extension
    pub fn from_path(path: &Path) -> Result<Self> {
        let ext = path.extension().and_then(|e| e.to_str()).ok_or_else(|| {
            Error::Format(FormatError::DetectionFailed(path.display().to_string()))
        })?;

        Self::from_extension(ext)
    }

    /// Detect format from file extension string
    pub fn from_extension(ext: &str) -> Result<Self> {
        match ext.to_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            "adt" => Ok(Self::Adt),
            "parquet" => Ok(Self::Parquet),
            "avro" => Ok(Self::Avro),
            "jsonl" | "ndjson" => Ok(Self::JsonLines),
            "arrow" => Ok(Self::Arrow),
            "json" => Ok(Self::Json),
            "jsonc" => Ok(Self::JsonCompact),
            "json5" => Ok(Self::Json5),
            "xlsx" => Ok(Self::Excel),
            "orc" => Ok(Self::Orc),
            _ => Err(Error::Format(FormatError::Unknown(ext.to_string()))),
        }
    }

    /// Parse format from string (for CLI arguments)
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            "adt" | "ascii-delimited" => Ok(Self::Adt),
            "parquet" => Ok(Self::Parquet),
            "avro" => Ok(Self::Avro),
            "jsonl" | "json-lines" | "ndjson" => Ok(Self::JsonLines),
            "arrow" => Ok(Self::Arrow),
            "json" => Ok(Self::Json),
            "jsonc" | "json-compact" => Ok(Self::JsonCompact),
            "json5" => Ok(Self::Json5),
            "excel" | "xlsx" => Ok(Self::Excel),
            "orc" => Ok(Self::Orc),
            _ => Err(Error::Format(FormatError::Unknown(s.to_string()))),
        }
    }

    /// Get the default file extension for this format
    pub fn default_extension(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Adt => "adt",
            Self::Parquet => "parquet",
            Self::Avro => "avro",
            Self::JsonLines => "jsonl",
            Self::Arrow => "arrow",
            Self::Json => "json",
            Self::JsonCompact => "jsonc",
            Self::Json5 => "json5",
            Self::Excel => "xlsx",
            Self::Orc => "orc",
        }
    }

    /// Check if format supports reading
    pub fn supports_reading(&self) -> bool {
        match self {
            Self::Csv
            | Self::Tsv
            | Self::Adt
            | Self::Parquet
            | Self::Avro
            | Self::JsonLines
            | Self::Arrow
            | Self::Json
            | Self::JsonCompact
            | Self::Json5 => true,
            Self::Excel | Self::Orc => false,
        }
    }

    /// Check if format supports writing
    pub fn supports_writing(&self) -> bool {
        true // All formats support writing
    }

    /// Check if format supports lazy reading
    pub fn supports_lazy_reading(&self) -> bool {
        match self {
            Self::Csv | Self::Adt | Self::Parquet | Self::JsonLines => true,
            Self::Tsv
            | Self::Avro
            | Self::Arrow
            | Self::Json
            | Self::JsonCompact
            | Self::Json5
            | Self::Excel
            | Self::Orc => false,
        }
    }

    /// Check if format supports streaming
    pub fn supports_streaming(&self) -> bool {
        match self {
            Self::Csv | Self::Tsv | Self::Adt | Self::JsonLines => true,
            Self::Parquet
            | Self::Avro
            | Self::Arrow
            | Self::Json
            | Self::JsonCompact
            | Self::Json5
            | Self::Excel
            | Self::Orc => false,
        }
    }

    /// Get human-readable format name
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Tsv => "TSV",
            Self::Adt => "ASCII Delimited Text",
            Self::Parquet => "Parquet",
            Self::Avro => "Avro",
            Self::JsonLines => "JSON Lines",
            Self::Arrow => "Arrow",
            Self::Json => "JSON",
            Self::JsonCompact => "JSON Compact",
            Self::Json5 => "JSON5",
            Self::Excel => "Excel",
            Self::Orc => "ORC",
        }
    }
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

impl FromStr for DataFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::from_str(s).map_err(|e| e.to_string())
    }
}

/// Options for format detection
#[derive(Debug, Clone)]
pub struct FormatOptions {
    /// Whether to use file content for detection if extension fails
    pub content_detection: bool,
    /// Maximum bytes to read for content detection
    pub detection_bytes: usize,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            content_detection: true,
            detection_bytes: 8192,
        }
    }
}

/// Detect format from file content (magic bytes)
pub fn detect_format_from_content(bytes: &[u8]) -> Option<DataFormat> {
    // Handle empty input
    if bytes.is_empty() {
        return Some(DataFormat::Csv);
    }

    // Parquet magic bytes: "PAR1" at start and end
    if bytes.len() >= 4 && &bytes[0..4] == b"PAR1" {
        return Some(DataFormat::Parquet);
    }

    // Avro magic bytes: "Obj\x01"
    if bytes.len() >= 4 && &bytes[0..4] == b"Obj\x01" {
        return Some(DataFormat::Avro);
    }

    // Arrow magic bytes: "ARROW1\x00\x00"
    if bytes.len() >= 8 && &bytes[0..6] == b"ARROW1" {
        return Some(DataFormat::Arrow);
    }

    // ORC magic bytes: "ORC"
    if bytes.len() >= 3 && &bytes[0..3] == b"ORC" {
        return Some(DataFormat::Orc);
    }

    // Try to detect text-based formats
    if let Ok(text) = std::str::from_utf8(bytes) {
        // Try to detect JSON formats first
        if serde_json::from_str::<serde_json::Value>(&text).is_ok() {
            if text.contains("//") || text.contains("/*") {
                return Some(DataFormat::Json5);
            }
            return Some(DataFormat::Json);
        }

        // Check for JsonLines format (each line is a JSON value)
        let lines: Vec<&str> = text.lines().take(5).collect();
        if lines.len() >= 1 {
            let mut valid_json_lines = 0;
            let mut total_lines = 0;
            for line in &lines {
                let line = line.trim();
                if !line.is_empty() {
                    total_lines += 1;
                    if serde_json::from_str::<serde_json::Value>(line).is_ok() {
                        valid_json_lines += 1;
                    }
                }
            }
            if valid_json_lines == total_lines && total_lines > 0 {
                return Some(DataFormat::JsonLines);
            }
        }

        // Try to detect CSV/TSV by counting delimiters in first few lines
        let lines: Vec<&str> = text.lines().take(5).collect();
        if lines.len() >= 2 {
            let comma_counts: Vec<usize> =
                lines.iter().map(|line| line.matches(',').count()).collect();
            let tab_counts: Vec<usize> = lines
                .iter()
                .map(|line| line.matches('\t').count())
                .collect();

            // Check consistency of delimiter counts
            let comma_consistent = comma_counts.windows(2).all(|w| w[0] == w[1] && w[0] > 0);
            let tab_consistent = tab_counts.windows(2).all(|w| w[0] == w[1] && w[0] > 0);

            if tab_consistent && (!comma_consistent || tab_counts[0] > comma_counts[0]) {
                return Some(DataFormat::Tsv);
            } else if comma_consistent {
                return Some(DataFormat::Csv);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_from_extension() {
        assert_eq!(DataFormat::from_extension("csv").unwrap(), DataFormat::Csv);
        assert_eq!(DataFormat::from_extension("CSV").unwrap(), DataFormat::Csv);
        assert_eq!(DataFormat::from_extension("tsv").unwrap(), DataFormat::Tsv);
        assert_eq!(DataFormat::from_extension("adt").unwrap(), DataFormat::Adt);
        assert_eq!(
            DataFormat::from_extension("parquet").unwrap(),
            DataFormat::Parquet
        );
        assert_eq!(
            DataFormat::from_extension("avro").unwrap(),
            DataFormat::Avro
        );
        assert_eq!(
            DataFormat::from_extension("jsonl").unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!(
            DataFormat::from_extension("ndjson").unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!(
            DataFormat::from_extension("arrow").unwrap(),
            DataFormat::Arrow
        );
        assert_eq!(
            DataFormat::from_extension("json").unwrap(),
            DataFormat::Json
        );
        assert_eq!(
            DataFormat::from_extension("jsonc").unwrap(),
            DataFormat::JsonCompact
        );
        assert_eq!(
            DataFormat::from_extension("json5").unwrap(),
            DataFormat::Json5
        );
        assert_eq!(
            DataFormat::from_extension("xlsx").unwrap(),
            DataFormat::Excel
        );
        assert_eq!(DataFormat::from_extension("orc").unwrap(), DataFormat::Orc);
        assert!(DataFormat::from_extension("unknown").is_err());
    }

    #[test]
    fn test_format_from_str() {
        assert_eq!(DataFormat::from_str("csv").unwrap(), DataFormat::Csv);
        assert_eq!(DataFormat::from_str("tsv").unwrap(), DataFormat::Tsv);
        assert_eq!(DataFormat::from_str("adt").unwrap(), DataFormat::Adt);
        assert_eq!(
            DataFormat::from_str("ascii-delimited").unwrap(),
            DataFormat::Adt
        );
        assert_eq!(
            DataFormat::from_str("parquet").unwrap(),
            DataFormat::Parquet
        );
        assert_eq!(DataFormat::from_str("avro").unwrap(), DataFormat::Avro);
        assert_eq!(
            DataFormat::from_str("jsonl").unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!(
            DataFormat::from_str("json-lines").unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!(
            DataFormat::from_str("ndjson").unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!(DataFormat::from_str("arrow").unwrap(), DataFormat::Arrow);
        assert_eq!(DataFormat::from_str("json").unwrap(), DataFormat::Json);
        assert_eq!(
            DataFormat::from_str("jsonc").unwrap(),
            DataFormat::JsonCompact
        );
        assert_eq!(
            DataFormat::from_str("json-compact").unwrap(),
            DataFormat::JsonCompact
        );
        assert_eq!(DataFormat::from_str("json5").unwrap(), DataFormat::Json5);
        assert_eq!(DataFormat::from_str("excel").unwrap(), DataFormat::Excel);
        assert_eq!(DataFormat::from_str("xlsx").unwrap(), DataFormat::Excel);
        assert_eq!(DataFormat::from_str("orc").unwrap(), DataFormat::Orc);
        assert!(DataFormat::from_str("invalid").is_err());
    }

    #[test]
    fn test_format_capabilities() {
        // Test reading support
        assert!(DataFormat::Csv.supports_reading());
        assert!(DataFormat::Tsv.supports_reading());
        assert!(DataFormat::Adt.supports_reading());
        assert!(DataFormat::Parquet.supports_reading());
        assert!(DataFormat::Avro.supports_reading());
        assert!(DataFormat::JsonLines.supports_reading());
        assert!(DataFormat::Arrow.supports_reading());
        assert!(DataFormat::Json.supports_reading());
        assert!(DataFormat::JsonCompact.supports_reading());
        assert!(DataFormat::Json5.supports_reading());
        assert!(!DataFormat::Excel.supports_reading());
        assert!(!DataFormat::Orc.supports_reading());

        // Test writing support (all should support)
        assert!(DataFormat::Csv.supports_writing());
        assert!(DataFormat::Tsv.supports_writing());
        assert!(DataFormat::Adt.supports_writing());
        assert!(DataFormat::Parquet.supports_writing());
        assert!(DataFormat::Avro.supports_writing());
        assert!(DataFormat::JsonLines.supports_writing());
        assert!(DataFormat::Arrow.supports_writing());
        assert!(DataFormat::Json.supports_writing());
        assert!(DataFormat::JsonCompact.supports_writing());
        assert!(DataFormat::Json5.supports_writing());
        assert!(DataFormat::Excel.supports_writing());
        assert!(DataFormat::Orc.supports_writing());

        // Test lazy reading support
        assert!(DataFormat::Csv.supports_lazy_reading());
        assert!(!DataFormat::Tsv.supports_lazy_reading());
        assert!(DataFormat::Adt.supports_lazy_reading());
        assert!(DataFormat::Parquet.supports_lazy_reading());
        assert!(!DataFormat::Avro.supports_lazy_reading());
        assert!(DataFormat::JsonLines.supports_lazy_reading());
        assert!(!DataFormat::Arrow.supports_lazy_reading());
        assert!(!DataFormat::Json.supports_lazy_reading());
        assert!(!DataFormat::JsonCompact.supports_lazy_reading());
        assert!(!DataFormat::Json5.supports_lazy_reading());
        assert!(!DataFormat::Excel.supports_lazy_reading());
        assert!(!DataFormat::Orc.supports_lazy_reading());

        // Test streaming support
        assert!(DataFormat::Csv.supports_streaming());
        assert!(DataFormat::Tsv.supports_streaming());
        assert!(DataFormat::Adt.supports_streaming());
        assert!(!DataFormat::Parquet.supports_streaming());
        assert!(!DataFormat::Avro.supports_streaming());
        assert!(DataFormat::JsonLines.supports_streaming());
        assert!(!DataFormat::Arrow.supports_streaming());
        assert!(!DataFormat::Json.supports_streaming());
        assert!(!DataFormat::JsonCompact.supports_streaming());
        assert!(!DataFormat::Json5.supports_streaming());
        assert!(!DataFormat::Excel.supports_streaming());
        assert!(!DataFormat::Orc.supports_streaming());
    }

    #[test]
    fn test_content_detection() {
        assert_eq!(
            detect_format_from_content(b"PAR1"),
            Some(DataFormat::Parquet)
        );
        assert_eq!(
            detect_format_from_content(b"Obj\x01"),
            Some(DataFormat::Avro)
        );
        assert_eq!(
            detect_format_from_content(b"ARROW1\x00\x00"),
            Some(DataFormat::Arrow)
        );
        assert_eq!(detect_format_from_content(b"ORC"), Some(DataFormat::Orc));

        assert_eq!(
            detect_format_from_content(b"[{\"a\": 1}]"),
            Some(DataFormat::Json)
        );
        assert_eq!(
            detect_format_from_content(b"{\"a\": 1}"),
            Some(DataFormat::Json)
        );
        assert_eq!(
            detect_format_from_content(b"{\"a\": 1}\n{\"b\": 2}"),
            Some(DataFormat::JsonLines)
        );

        assert_eq!(
            detect_format_from_content(b"a,b,c\n1,2,3\n4,5,6"),
            Some(DataFormat::Csv)
        );
        assert_eq!(
            detect_format_from_content(b"a\tb\tc\n1\t2\t3\n4\t5\t6"),
            Some(DataFormat::Tsv)
        );

        assert_eq!(detect_format_from_content(b"random data"), None);
    }

    #[test]
    fn test_from_path() {
        use std::path::Path;
        assert_eq!(
            DataFormat::from_path(Path::new("file.csv")).unwrap(),
            DataFormat::Csv
        );
        assert_eq!(
            DataFormat::from_path(Path::new("file.CSV")).unwrap(),
            DataFormat::Csv
        );
        assert_eq!(
            DataFormat::from_path(Path::new("file.tsv")).unwrap(),
            DataFormat::Tsv
        );
        assert_eq!(
            DataFormat::from_path(Path::new("file.parquet")).unwrap(),
            DataFormat::Parquet
        );
        assert_eq!(
            DataFormat::from_path(Path::new("file.jsonl")).unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!(
            DataFormat::from_path(Path::new("file.json")).unwrap(),
            DataFormat::Json
        );
        assert_eq!(
            DataFormat::from_path(Path::new("file.xlsx")).unwrap(),
            DataFormat::Excel
        );
        assert!(DataFormat::from_path(Path::new("file")).is_err());
        assert!(DataFormat::from_path(Path::new("file.unknown")).is_err());
    }

    #[test]
    fn test_default_extension() {
        assert_eq!(DataFormat::Csv.default_extension(), "csv");
        assert_eq!(DataFormat::Tsv.default_extension(), "tsv");
        assert_eq!(DataFormat::Adt.default_extension(), "adt");
        assert_eq!(DataFormat::Parquet.default_extension(), "parquet");
        assert_eq!(DataFormat::Avro.default_extension(), "avro");
        assert_eq!(DataFormat::JsonLines.default_extension(), "jsonl");
        assert_eq!(DataFormat::Arrow.default_extension(), "arrow");
        assert_eq!(DataFormat::Json.default_extension(), "json");
        assert_eq!(DataFormat::JsonCompact.default_extension(), "jsonc");
        assert_eq!(DataFormat::Json5.default_extension(), "json5");
        assert_eq!(DataFormat::Excel.default_extension(), "xlsx");
        assert_eq!(DataFormat::Orc.default_extension(), "orc");
    }

    #[test]
    fn test_display_name() {
        assert_eq!(DataFormat::Csv.display_name(), "CSV");
        assert_eq!(DataFormat::Tsv.display_name(), "TSV");
        assert_eq!(DataFormat::Adt.display_name(), "ASCII Delimited Text");
        assert_eq!(DataFormat::Parquet.display_name(), "Parquet");
        assert_eq!(DataFormat::Avro.display_name(), "Avro");
        assert_eq!(DataFormat::JsonLines.display_name(), "JSON Lines");
        assert_eq!(DataFormat::Arrow.display_name(), "Arrow");
        assert_eq!(DataFormat::Json.display_name(), "JSON");
        assert_eq!(DataFormat::JsonCompact.display_name(), "JSON Compact");
        assert_eq!(DataFormat::Json5.display_name(), "JSON5");
        assert_eq!(DataFormat::Excel.display_name(), "Excel");
        assert_eq!(DataFormat::Orc.display_name(), "ORC");
    }

    #[test]
    fn test_display_trait() {
        assert_eq!(format!("{}", DataFormat::Csv), "CSV");
        assert_eq!(format!("{}", DataFormat::JsonLines), "JSON Lines");
        assert_eq!(format!("{}", DataFormat::Excel), "Excel");
    }

    #[test]
    fn test_from_str_trait() {
        assert_eq!("csv".parse::<DataFormat>().unwrap(), DataFormat::Csv);
        assert_eq!(
            "json-lines".parse::<DataFormat>().unwrap(),
            DataFormat::JsonLines
        );
        assert_eq!("excel".parse::<DataFormat>().unwrap(), DataFormat::Excel);
        assert!("invalid".parse::<DataFormat>().is_err());
    }

    #[test]
    fn test_format_options_default() {
        let opts = FormatOptions::default();
        assert!(opts.content_detection);
        assert_eq!(opts.detection_bytes, 8192);
    }
}
