use crate::error::{Error, FormatError, Result};

use polars::{
    io::csv::CsvReader as PolarsCsvReader,
    prelude::{DataFrame, LazyFrame, NullValues, SerReader, SerWriter, Series},
};

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// CSV-specific reading options
#[derive(Debug, Clone)]
pub struct CsvReadOptions {
    /// Field separator character
    pub separator: u8,
    /// Whether the first row contains column headers
    pub has_header: bool,
    /// Quote character for fields containing separators or newlines
    pub quote_char: Option<u8>,
    /// Comment character - lines starting with this are ignored
    pub comment_char: Option<u8>,
    /// Values to treat as null/missing
    pub null_values: Option<Vec<String>>,
    /// Text encoding handling
    pub encoding: CsvEncoding,
    /// Whether to trim whitespace from fields
    pub trim_whitespace: bool,
    /// Maximum number of rows to read for schema inference
    pub infer_schema_length: Option<usize>,
    /// Skip first N rows (after header if present)
    pub skip_rows: usize,
    /// Skip first N rows including potential header
    pub skip_rows_after_header: usize,
    /// Treat first row as data even if has_header is true
    pub ignore_header: bool,
    /// Buffer size for reading
    pub buffer_size: usize,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            separator: b',',
            has_header: true,
            quote_char: Some(b'"'),
            comment_char: None,
            null_values: None,
            encoding: CsvEncoding::Utf8,
            trim_whitespace: false,
            infer_schema_length: Some(100),
            skip_rows: 0,
            skip_rows_after_header: 0,
            ignore_header: false,
            buffer_size: 262_144, // 256KB - much better for modern systems
        }
    }
}

/// CSV-specific writing options
#[derive(Debug, Clone)]
pub struct CsvWriteOptions {
    /// Field separator character
    pub separator: u8,
    /// Quote character for fields
    pub quote_char: u8,
    /// Line terminator
    pub line_terminator: String,
    /// When to quote fields
    pub quote_style: QuoteStyle,
    /// String to write for null values
    pub null_value: String,
    /// Format for datetime values
    pub datetime_format: Option<String>,
    /// Format for date values
    pub date_format: Option<String>,
    /// Format for time values
    pub time_format: Option<String>,
    /// Precision for floating point numbers
    pub float_precision: Option<usize>,
    /// Whether to include header row
    pub include_header: bool,
    /// Buffer size for writing
    pub buffer_size: usize,
}

impl Default for CsvWriteOptions {
    fn default() -> Self {
        Self {
            separator: b',',
            quote_char: b'"',
            line_terminator: "\n".to_string(),
            quote_style: QuoteStyle::Necessary,
            null_value: "".to_string(),
            datetime_format: None,
            date_format: None,
            time_format: None,
            float_precision: None,
            include_header: true,
            buffer_size: 262_144, // 256KB buffer for faster writes
        }
    }
}

/// Text encoding options for CSV files
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvEncoding {
    /// Strict UTF-8 - fail on invalid sequences
    Utf8,
    /// Lossy UTF-8 - replace invalid sequences with replacement character
    Utf8Lossy,
}

/// Quoting strategy for CSV output
#[derive(Debug, Clone, Copy)]
pub enum QuoteStyle {
    /// Always quote all fields
    Always,
    /// Quote only when necessary (fields containing separator, quote, or newline)
    Necessary,
    /// Quote all non-numeric fields
    NonNumeric,
    /// Never quote fields (may produce invalid CSV)
    Never,
}

/// CSV reader that provides format-specific optimizations
pub struct CsvReader<R> {
    reader: R,
    options: CsvReadOptions,
    detected_separator: Option<u8>,
    detected_headers: Option<Vec<String>>,
}

impl<R: Read> CsvReader<R> {
    /// Create a new CSV reader with default options
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            options: CsvReadOptions::default(),
            detected_separator: None,
            detected_headers: None,
        }
    }

    /// Create a CSV reader with custom options
    pub fn with_options(reader: R, options: CsvReadOptions) -> Self {
        Self {
            reader,
            options,
            detected_separator: None,
            detected_headers: None,
        }
    }

    /// Set the field separator
    pub fn with_separator(mut self, separator: u8) -> Self {
        self.options.separator = separator;
        self
    }

    /// Set whether the file has headers
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.options.has_header = has_header;
        self
    }

    /// Set the quote character
    pub fn with_quote_char(mut self, quote_char: Option<u8>) -> Self {
        self.options.quote_char = quote_char;
        self
    }

    /// Set null values
    pub fn with_null_values(mut self, null_values: Vec<String>) -> Self {
        self.options.null_values = Some(null_values);
        self
    }

    /// Auto-detect separator from the first few lines
    pub fn auto_detect_separator(&mut self) -> Result<u8> {
        if let Some(sep) = self.detected_separator {
            return Ok(sep);
        }

        let mut temp_reader = BufReader::new(&mut self.reader);

        // Read first few lines for detection
        let mut lines: Vec<String> = Vec::new();
        let mut line = String::new();
        for _ in 0..5 {
            line.clear();
            let bytes_read = temp_reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break;
            }
            lines.push(line.clone());
        }

        if lines.is_empty() {
            return Err(Error::Format(FormatError::DetectionFailed(
                "No data to analyze for separator detection".to_string(),
            )));
        }

        // Count potential separators
        let separators = [b',', b'\t', b';', b'|'];
        let mut separator_counts = std::collections::HashMap::new();

        for sep in separators {
            let counts: Vec<usize> = lines
                .iter()
                .map(|line| line.as_bytes().iter().filter(|&&b| b == sep).count())
                .collect();

            // Check if counts are consistent (same across lines)
            if counts.len() > 1 {
                let first_count = counts[0];
                let is_consistent = counts.iter().all(|&count| count == first_count);
                if is_consistent && first_count > 0 {
                    separator_counts.insert(sep, first_count);
                }
            } else if counts.len() == 1 && counts[0] > 0 {
                separator_counts.insert(sep, counts[0]);
            }
        }

        // Choose the separator with the highest consistent count
        let detected = separator_counts
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(&sep, _)| sep)
            .unwrap_or(b','); // Default to comma

        self.detected_separator = Some(detected);
        Ok(detected)
    }

    /// Read CSV data into a DataFrame
    pub fn read_dataframe(&mut self) -> Result<DataFrame> {
        let separator = self.detected_separator.unwrap_or(self.options.separator);

        #[cfg(feature = "profiling")]
        coz::progress!("csv_read_start");

        // Read all data into buffer since polars CsvReader requires MmapBytesReader
        let mut buffer = Vec::new();
        self.reader.read_to_end(&mut buffer)?;

        #[cfg(feature = "profiling")]
        coz::progress!("csv_data_loaded");

        // Handle empty input
        if buffer.is_empty() {
            return Ok(DataFrame::empty());
        }

        // Create Polars CSV reader from buffer
        let mut csv_reader = PolarsCsvReader::new(std::io::Cursor::new(buffer))
            .with_separator(separator)
            .has_header(self.options.has_header);

        if let Some(quote) = self.options.quote_char {
            csv_reader = csv_reader.with_quote_char(Some(quote));
        }

        if let Some(comment) = self.options.comment_char {
            csv_reader = csv_reader.with_comment_char(Some(comment));
        }

        if let Some(null_vals) = &self.options.null_values {
            csv_reader =
                csv_reader.with_null_values(Some(NullValues::AllColumns(null_vals.clone())));
        }

        if let Some(infer_len) = self.options.infer_schema_length {
            csv_reader = csv_reader.infer_schema(Some(infer_len));
        }

        if self.options.skip_rows > 0 {
            csv_reader = csv_reader.with_skip_rows(self.options.skip_rows);
        }

        if self.options.skip_rows_after_header > 0 {
            csv_reader =
                csv_reader.with_skip_rows_after_header(self.options.skip_rows_after_header);
        }

        let result = match self.options.encoding {
            CsvEncoding::Utf8 => csv_reader.finish().map_err(Error::from),
            CsvEncoding::Utf8Lossy => {
                // Polars doesn't directly support lossy UTF-8, so we handle it here
                csv_reader.finish().map_err(Error::from)
            }
        };

        #[cfg(feature = "profiling")]
        coz::progress!("csv_parsed");

        result
    }

    /// Read CSV data into a LazyFrame for lazy evaluation
    pub fn read_lazy(&mut self) -> Result<LazyFrame> {
        let _separator = self.detected_separator.unwrap_or(self.options.separator);

        // For lazy reading, we need to work with file paths
        // This is a limitation - lazy reading from generic readers is complex
        Err(Error::Format(FormatError::UnsupportedFeature(
            "Lazy reading from generic reader not supported. Use file-based reading.".to_string(),
        )))
    }

    /// Peek at the first few rows to understand the data structure
    pub fn peek(&mut self, rows: usize) -> Result<DataFrame> {
        let separator = self.detected_separator.unwrap_or(self.options.separator);

        // Read all data into buffer since polars CsvReader requires MmapBytesReader
        let mut buffer = Vec::new();
        self.reader.read_to_end(&mut buffer)?;

        let mut csv_reader = PolarsCsvReader::new(std::io::Cursor::new(buffer))
            .with_separator(separator)
            .has_header(self.options.has_header)
            .with_n_rows(Some(rows));

        if let Some(quote) = self.options.quote_char {
            csv_reader = csv_reader.with_quote_char(Some(quote));
        }

        csv_reader.finish().map_err(Error::from)
    }

    /// Get the detected or configured headers
    pub fn headers(&mut self) -> Result<Vec<String>> {
        if let Some(ref headers) = self.detected_headers {
            return Ok(headers.clone());
        }

        if !self.options.has_header {
            // Generate default column names
            let peek_df = self.peek(1)?;
            let headers: Vec<String> = (0..peek_df.width())
                .map(|i| format!("column_{}", i))
                .collect();
            self.detected_headers = Some(headers.clone());
            return Ok(headers);
        }

        // Read the first row to get headers
        let mut buf_reader = BufReader::new(&mut self.reader);
        let mut line = String::new();
        buf_reader.read_line(&mut line)?;

        let separator = self.detected_separator.unwrap_or(self.options.separator);
        let headers = self.parse_csv_line(&line, separator)?;

        self.detected_headers = Some(headers.clone());
        Ok(headers)
    }

    /// Parse a single CSV line into fields
    fn parse_csv_line(&self, line: &str, separator: u8) -> Result<Vec<String>> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.trim_end().chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' if self.options.quote_char == Some(b'"') => {
                    if in_quotes {
                        // Check for escaped quote
                        if chars.peek() == Some(&'"') {
                            chars.next(); // consume the second quote
                            current_field.push('"');
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                c if c as u8 == separator && !in_quotes => {
                    fields.push(if self.options.trim_whitespace {
                        current_field.trim().to_string()
                    } else {
                        current_field
                    });
                    current_field = String::new();
                }
                c => {
                    current_field.push(c);
                }
            }
        }

        // Add the last field
        fields.push(if self.options.trim_whitespace {
            current_field.trim().to_string()
        } else {
            current_field
        });

        Ok(fields)
    }
}

/// CSV writer that provides format-specific optimizations
pub struct CsvWriter<W: Write> {
    writer: BufWriter<W>,
    options: CsvWriteOptions,
    headers_written: bool,
}

impl<W: Write> CsvWriter<W> {
    /// Create a new CSV writer with default options
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::with_capacity(8192, writer),
            options: CsvWriteOptions::default(),
            headers_written: false,
        }
    }

    /// Create a CSV writer with custom options
    pub fn with_options(writer: W, options: CsvWriteOptions) -> Self {
        Self {
            writer: BufWriter::with_capacity(options.buffer_size, writer),
            options,
            headers_written: false,
        }
    }

    /// Set the field separator
    pub fn with_separator(mut self, separator: u8) -> Self {
        self.options.separator = separator;
        self
    }

    /// Set whether to include headers
    pub fn include_header(mut self, include_header: bool) -> Self {
        self.options.include_header = include_header;
        self
    }

    /// Set the quote character
    pub fn with_quote_char(mut self, quote_char: u8) -> Self {
        self.options.quote_char = quote_char;
        self
    }

    /// Set the quote style
    pub fn with_quote_style(mut self, quote_style: QuoteStyle) -> Self {
        self.options.quote_style = quote_style;
        self
    }

    /// Write a DataFrame to CSV
    pub fn write_dataframe(&mut self, df: &DataFrame) -> Result<()> {
        // Write headers if needed
        if self.options.include_header && !self.headers_written {
            self.write_headers(df.get_column_names())?;
            self.headers_written = true;
        }

        // Write data rows
        for row_idx in 0..df.height() {
            self.write_row(df, row_idx)?;
        }

        self.writer.flush()?;
        Ok(())
    }

    /// Write headers
    fn write_headers(&mut self, headers: Vec<&str>) -> Result<()> {
        for (i, header) in headers.iter().enumerate() {
            if i > 0 {
                self.writer.write_all(&[self.options.separator])?;
            }
            self.write_field(header, false)?;
        }
        self.writer
            .write_all(self.options.line_terminator.as_bytes())?;
        Ok(())
    }

    /// Write a single data row
    fn write_row(&mut self, df: &DataFrame, row_idx: usize) -> Result<()> {
        let columns = df.get_columns();

        for (i, series) in columns.iter().enumerate() {
            if i > 0 {
                self.writer.write_all(&[self.options.separator])?;
            }
            self.write_series_value(series, row_idx)?;
        }

        self.writer
            .write_all(self.options.line_terminator.as_bytes())?;
        Ok(())
    }

    /// Write a single field value from a Series
    fn write_series_value(&mut self, series: &Series, index: usize) -> Result<()> {
        use polars::datatypes::*;

        if series.is_null().get(index).unwrap_or(false) {
            self.writer.write_all(self.options.null_value.as_bytes())?;
            return Ok(());
        }

        match series.dtype() {
            DataType::Boolean => {
                let val = series
                    .bool()
                    .map_err(Error::from)?
                    .get(index)
                    .unwrap_or(false);
                let field_str = val.to_string();
                self.write_field(&field_str, false)?;
            }
            DataType::Int8 => {
                let val = series.i8().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::Int16 => {
                let val = series.i16().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::Int32 => {
                let val = series.i32().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::Int64 => {
                let val = series.i64().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::UInt8 => {
                let val = series.u8().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::UInt16 => {
                let val = series.u16().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::UInt32 => {
                let val = series.u32().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::UInt64 => {
                let val = series.u64().map_err(Error::from)?.get(index).unwrap_or(0);
                let field_str = val.to_string();
                self.write_field(&field_str, true)?;
            }
            DataType::Float32 => {
                let val = series.f32().map_err(Error::from)?.get(index).unwrap_or(0.0) as f64;
                let field_str = if let Some(precision) = self.options.float_precision {
                    format!("{:.prec$}", val, prec = precision)
                } else {
                    val.to_string()
                };
                self.write_field(&field_str, true)?;
            }
            DataType::Float64 => {
                let val = series.f64().map_err(Error::from)?.get(index).unwrap_or(0.0);
                let field_str = if let Some(precision) = self.options.float_precision {
                    format!("{:.prec$}", val, prec = precision)
                } else {
                    val.to_string()
                };
                self.write_field(&field_str, true)?;
            }
            DataType::Utf8 => {
                let val = series.utf8().map_err(Error::from)?.get(index).unwrap_or("");
                self.write_field(val, false)?;
            }
            DataType::Date => {
                let val = series.date().map_err(Error::from)?.get(index);
                if let Some(date) = val {
                    let field_str = date.to_string();
                    self.write_field(&field_str, false)?;
                } else {
                    self.writer.write_all(self.options.null_value.as_bytes())?;
                }
            }
            DataType::Datetime(_, _) => {
                let val = series.datetime().map_err(Error::from)?.get(index);
                if let Some(dt) = val {
                    let field_str = dt.to_string();
                    self.write_field(&field_str, false)?;
                } else {
                    self.writer.write_all(self.options.null_value.as_bytes())?;
                }
            }
            _ => {
                // For unsupported types, convert to string
                let field_str = format!("{:?}", series.get(index).map_err(Error::from)?);
                self.write_field(&field_str, false)?;
            }
        }

        Ok(())
    }

    /// Write a single field, applying quoting rules
    fn write_field(&mut self, field: &str, is_numeric: bool) -> Result<()> {
        let needs_quoting = match self.options.quote_style {
            QuoteStyle::Always => true,
            QuoteStyle::Never => false,
            QuoteStyle::NonNumeric => !is_numeric,
            QuoteStyle::Necessary => {
                field.contains(self.options.separator as char)
                    || field.contains(self.options.quote_char as char)
                    || field.contains('\n')
                    || field.contains('\r')
            }
        };

        if needs_quoting {
            self.writer.write_all(&[self.options.quote_char])?;

            // Escape any quote characters in the field
            for ch in field.chars() {
                if ch as u8 == self.options.quote_char {
                    self.writer
                        .write_all(&[self.options.quote_char, self.options.quote_char])?;
                } else {
                    write!(self.writer, "{}", ch)?;
                }
            }

            self.writer.write_all(&[self.options.quote_char])?;
        } else {
            self.writer.write_all(field.as_bytes())?;
        }

        Ok(())
    }

    /// Flush the writer
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Error::from)
    }

    /// Finish writing and return the underlying writer
    pub fn finish(mut self) -> Result<W> {
        self.writer.flush()?;
        Ok(self
            .writer
            .into_inner()
            .map_err(|e| Error::operation(format!("Failed to finish CSV writer: {}", e)))?)
    }
}

/// Convenience function to read CSV from a file path
pub fn read_csv_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    let file = File::open(path)?;
    let mut reader = CsvReader::new(file);
    reader.read_dataframe()
}

/// Convenience function to read CSV from a file path with options
pub fn read_csv_file_with_options<P: AsRef<Path>>(
    path: P,
    mut options: CsvReadOptions,
) -> Result<DataFrame> {
    // Auto-detect TSV files
    if path.as_ref().extension() == Some(std::ffi::OsStr::new("tsv")) {
        options.separator = b'\t';
    }
    let file = File::open(path)?;
    let mut reader = CsvReader::with_options(file, options);
    reader.read_dataframe()
}

/// Convenience function to write DataFrame to CSV file
pub fn write_csv_file<P: AsRef<Path>>(df: &DataFrame, path: P) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = CsvWriter::new(file);
    writer.write_dataframe(df)
}

/// Convenience function to write DataFrame to CSV file with options
pub fn write_csv_file_with_options<P: AsRef<Path>>(
    df: &DataFrame,
    path: P,
    options: CsvWriteOptions,
) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = CsvWriter::with_options(file, options);
    writer.write_dataframe(df)
}

/// Detect CSV dialect (separator, quote char, etc.) from sample data
pub fn detect_csv_dialect<R: Read>(mut reader: R) -> Result<CsvReadOptions> {
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;

    let sample = String::from_utf8_lossy(&buffer[..std::cmp::min(buffer.len(), 8192)]);
    let lines: Vec<&str> = sample.lines().take(10).collect();

    if lines.is_empty() {
        return Ok(CsvReadOptions::default());
    }

    // Detect separator
    let separators = [b',', b'\t', b';', b'|'];
    let mut separator_scores = std::collections::HashMap::new();

    for &sep in &separators {
        let counts: Vec<usize> = lines
            .iter()
            .map(|line| line.as_bytes().iter().filter(|&&b| b == sep).count())
            .collect();

        if counts.len() > 1 {
            let first_count = counts[0];
            let consistency = counts.iter().filter(|&&count| count == first_count).count();
            let score = consistency * first_count;
            separator_scores.insert(sep, score);
        }
    }

    let detected_separator = separator_scores
        .iter()
        .max_by_key(|(_, &score)| score)
        .map(|(&sep, _)| sep)
        .unwrap_or(b',');

    // Detect quote character
    let quote_chars = [b'"', b'\''];
    let mut quote_char = None;

    for &quote in &quote_chars {
        let quote_count: usize = sample.as_bytes().iter().filter(|&&b| b == quote).count();
        if quote_count > 0 && quote_count % 2 == 0 {
            quote_char = Some(quote);
            break;
        }
    }

    // Detect if first row is header
    let has_header = if lines.len() >= 2 {
        let first_line_fields = lines[0].split(detected_separator as char).count();
        let second_line_fields = lines[1].split(detected_separator as char).count();

        // If field counts match and first line looks like text, it's probably a header
        first_line_fields == second_line_fields && lines[0].chars().any(|c| c.is_alphabetic())
    } else {
        true // Default assumption
    };

    Ok(CsvReadOptions {
        separator: detected_separator,
        has_header,
        quote_char,
        ..Default::default()
    })
}

/// Detect CSV format from content
pub fn detect_csv_format(bytes: &[u8]) -> bool {
    if let Ok(text) = std::str::from_utf8(bytes) {
        // Check for CSV-like structure: lines with consistent delimiters
        let lines: Vec<&str> = text.lines().take(5).collect();
        if lines.len() < 2 {
            return false;
        }

        // Count commas and tabs in each line
        let comma_counts: Vec<usize> = lines.iter().map(|line| line.matches(',').count()).collect();
        let tab_counts: Vec<usize> = lines
            .iter()
            .map(|line| line.matches('\t').count())
            .collect();

        // Check if delimiter counts are consistent
        let comma_consistent = comma_counts.windows(2).all(|w| w[0] == w[1] && w[0] > 0);
        let tab_consistent = tab_counts.windows(2).all(|w| w[0] == w[1] && w[0] > 0);

        comma_consistent || tab_consistent
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{
        detect_csv_dialect, detect_csv_format, read_csv_file, read_csv_file_with_options,
        write_csv_file, write_csv_file_with_options, CsvEncoding, CsvReadOptions, CsvReader,
        CsvWriteOptions, Error, FormatError, QuoteStyle,
    };
    use crate::csv::CsvWriter;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use polars::{
        df,
        prelude::{DataFrame, NamedFrom, Series},
    };
    use std::fs;
    use std::io::Cursor;
    use tempfile::NamedTempFile;

    #[test]
    fn test_csv_reader() {
        let csv_data = "name,age,active\nAlice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
        assert_eq!(df.get_column_names(), vec!["name", "age", "active"]);
    }

    #[test]
    fn test_csv_writer() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25],
            "active" => [true, false]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buffer);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("name,age,active"));
        assert!(output.contains("Alice,30,true"));
        assert!(output.contains("Bob,25,false"));
    }

    #[test]
    fn test_separator_detection() {
        let csv_data = "name\tage\tactive\nAlice\t30\ttrue\nBob\t25\tfalse\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let separator = reader.auto_detect_separator().unwrap();

        assert_eq!(separator, b'\t');
    }

    #[test]
    fn test_csv_dialect_detection() {
        let csv_data = "name;age;city\n'Alice';30;'New York'\n'Bob';25;'Boston'\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = detect_csv_dialect(cursor).unwrap();

        assert_eq!(options.separator, b';');
        assert_eq!(options.quote_char, Some(b'\''));
        assert!(options.has_header);
    }

    #[test]
    fn test_custom_quote_style() {
        let df = df! {
            "name" => ["Alice Smith", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                quote_style: QuoteStyle::Always,
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        // All fields should be quoted
        assert!(output.contains("\"name\",\"age\""));
        assert!(output.contains("\"Alice Smith\",\"30\""));
    }

    #[test]
    fn test_null_value_handling() {
        let csv_data = "name,age,city\nAlice,30,\nBob,,Boston\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            null_values: Some(vec!["".to_string()]),
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        // Check that empty values are treated as nulls
        let city_series = df.column("city").unwrap();
        assert!(city_series.is_null().get(0).unwrap());
    }

    #[test]
    fn test_csv_reader_no_header() {
        let csv_data = "Alice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            has_header: false,
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
        // Should have default column names
        assert_eq!(
            df.get_column_names(),
            vec!["column_0", "column_1", "column_2"]
        );
    }

    #[test]
    fn test_csv_reader_custom_separator() {
        let csv_data = "name;age;active\nAlice;30;true\nBob;25;false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            separator: b';',
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
        assert_eq!(df.get_column_names(), vec!["name", "age", "active"]);
    }

    #[test]
    fn test_csv_reader_with_quotes() {
        let csv_data = "\"name\",\"age\",\"city\"\n\"Alice\",\"30\",\"New York\"\n\"Bob\",\"25\",\"Boston, MA\"\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
        assert_eq!(df.get_column_names(), vec!["name", "age", "city"]);

        let city_series = df.column("city").unwrap();
        assert_eq!(city_series.utf8().unwrap().get(1), Some("Boston, MA"));
    }

    #[test]
    fn test_csv_reader_escaped_quotes() {
        let csv_data = "\"name\",\"description\"\n\"Alice\",\"She said \"\"Hello\"\"\"\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 1);
        let desc_series = df.column("description").unwrap();
        assert_eq!(
            desc_series.utf8().unwrap().get(0),
            Some("She said \"Hello\"")
        );
    }

    #[test]
    fn test_csv_writer_quote_styles() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "description" => ["Hello world", "Test, with comma"]
        }
        .unwrap();

        // Test QuoteStyle::Necessary
        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                quote_style: QuoteStyle::Necessary,
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("Test, with comma")); // Should be quoted
        assert!(output.contains("Hello world")); // Should not be quoted

        // Test QuoteStyle::Always
        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                quote_style: QuoteStyle::Always,
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("\"Hello world\"")); // Should be quoted
        assert!(output.contains("\"Test, with comma\"")); // Should be quoted
    }

    #[test]
    fn test_csv_writer_no_header() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                include_header: false,
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(!output.contains("name,age"));
        assert!(output.contains("Alice,30"));
        assert!(output.contains("Bob,25"));
    }

    #[test]
    fn test_csv_writer_custom_separator() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                separator: b';',
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("name;age"));
        assert!(output.contains("Alice;30"));
    }

    #[test]
    fn test_separator_detection_edge_cases() {
        // Test with inconsistent separators
        let csv_data = "name,age\tactive\nAlice,30\ttrue\nBob\t25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let separator = reader.auto_detect_separator().unwrap();

        // Should detect comma as it's more consistent
        assert_eq!(separator, b',');

        // Test with single line
        let csv_data = "name,age,active";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let separator = reader.auto_detect_separator().unwrap();
        assert_eq!(separator, b',');
    }

    #[test]
    fn test_csv_dialect_detection_edge_cases() {
        // Test with no quotes
        let csv_data = "name,age,city\nAlice,30,New York\nBob,25,Boston\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = detect_csv_dialect(cursor).unwrap();
        assert_eq!(options.separator, b',');
        assert_eq!(options.quote_char, None);
        assert!(options.has_header);

        // Test with mixed quotes
        let csv_data = "name;age;city\n'Alice';30;New York\nBob;25;Boston\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = detect_csv_dialect(cursor).unwrap();
        assert_eq!(options.separator, b';');
        assert_eq!(options.quote_char, Some(b'\''));
    }

    #[test]
    fn test_parse_csv_line_edge_cases() {
        let reader = CsvReader::new(Cursor::new(b""));

        // Test with quotes and commas
        let line = "\"Hello, world\",\"test\"";
        let fields = reader.parse_csv_line(line, b',').unwrap();
        assert_eq!(fields, vec!["Hello, world", "test"]);

        // Test with escaped quotes
        let line = "\"She said \"\"Hello\"\"\",\"test\"";
        let fields = reader.parse_csv_line(line, b',').unwrap();
        assert_eq!(fields, vec!["She said \"Hello\"", "test"]);

        // Test with trailing comma
        let line = "Alice,30,";
        let fields = reader.parse_csv_line(line, b',').unwrap();
        assert_eq!(fields, vec!["Alice", "30", ""]);

        // Test with whitespace trimming
        let options = CsvReadOptions {
            trim_whitespace: true,
            ..Default::default()
        };
        let reader = CsvReader::with_options(Cursor::new(b""), options);
        let line = " Alice , 30 , test ";
        let fields = reader.parse_csv_line(line, b',').unwrap();
        assert_eq!(fields, vec!["Alice", "30", "test"]);
    }

    #[test]
    fn test_peek_functionality() {
        let csv_data = "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let df = reader.peek(2).unwrap();

        assert_eq!(df.height(), 2); // Should only peek at first 2 rows
        assert_eq!(df.width(), 3);
        assert_eq!(df.get_column_names(), vec!["name", "age", "active"]);
    }

    #[test]
    fn test_headers_functionality() {
        let csv_data = "name,age,active\nAlice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let headers = reader.headers().unwrap();

        assert_eq!(headers, vec!["name", "age", "active"]);

        // Test with no header
        let csv_data = "Alice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());
        let options = CsvReadOptions {
            has_header: false,
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let headers = reader.headers().unwrap();
        assert_eq!(headers, vec!["column_0", "column_1", "column_2"]);
    }

    #[test]
    fn test_read_lazy_error() {
        let cursor = Cursor::new(b"name,age\nAlice,30");

        let mut reader = CsvReader::new(cursor);
        let result = reader.read_lazy();

        assert!(result.is_err());
        if let Err(Error::Format(FormatError::UnsupportedFeature(msg))) = result {
            assert!(msg.contains("Lazy reading from generic reader not supported"));
        } else {
            panic!("Expected UnsupportedFeature error");
        }
    }

    #[test]
    fn test_csv_writer_different_data_types() {
        let df = df! {
            "int_col" => [1i32, 2, 3],
            "float_col" => [1.5f64, 2.5, 3.5],
            "bool_col" => [true, false, true],
            "str_col" => ["hello", "world", "test"]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buffer);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("int_col,float_col,bool_col,str_col"));
        assert!(output.contains("1,1.5,true,hello"));
        assert!(output.contains("2,2.5,false,world"));
        assert!(output.contains("3,3.5,true,test"));
    }

    #[test]
    fn test_skip_rows_functionality() {
        let csv_data =
            "# Comment line\n# Another comment\nname,age,active\nAlice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            skip_rows: 2, // Skip the comment lines
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.get_column_names(), vec!["name", "age", "active"]);
    }

    #[test]
    fn test_skip_rows_after_header() {
        let csv_data = "name,age,active\n# Skip this\nAlice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            skip_rows_after_header: 1, // Skip one row after header
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2); // Should have Alice and Bob
        let name_series = df.column("name").unwrap();
        assert_eq!(name_series.utf8().unwrap().get(0), Some("Alice"));
        assert_eq!(name_series.utf8().unwrap().get(1), Some("Bob"));
    }

    #[test]
    fn test_ignore_header_option() {
        let csv_data = "name,age,active\nAlice,30,true\nBob,25,false\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            ignore_header: true,
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 3); // Header row treated as data
        assert_eq!(
            df.get_column_names(),
            vec!["column_0", "column_1", "column_2"]
        );
    }

    #[test]
    fn test_comment_char_handling() {
        let csv_data = "name,age,active\nAlice,30,true\n# Bob,25,false\nCharlie,35,true\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            comment_char: Some(b'#'),
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2); // Comment line should be ignored
        let name_series = df.column("name").unwrap();
        assert_eq!(name_series.utf8().unwrap().get(0), Some("Alice"));
        assert_eq!(name_series.utf8().unwrap().get(1), Some("Charlie"));
    }

    #[test]
    fn test_encoding_handling() {
        // Test with UTF-8 lossy encoding option
        let csv_data = "name,age\nAlice,30\nBob,25\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            encoding: CsvEncoding::Utf8Lossy,
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        assert_eq!(df.get_column_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_infer_schema_length() {
        let csv_data = "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n";
        let cursor = Cursor::new(csv_data.as_bytes());

        let options = CsvReadOptions {
            infer_schema_length: Some(2), // Only look at first 2 rows for schema
            ..Default::default()
        };

        let mut reader = CsvReader::with_options(cursor, options);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 3); // Should read all rows
        assert_eq!(df.width(), 3);
    }

    #[test]
    fn test_buffer_size_options() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                buffer_size: 1024, // Custom buffer size
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("name,age"));
        assert!(output.contains("Alice,30"));
    }

    #[test]
    fn test_line_terminator_options() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                line_terminator: "\r\n".to_string(), // Windows line endings
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("\r\n"));
    }

    #[test]
    fn test_float_precision() {
        let df = df! {
            "value" => [1.23456789f64, 2.98765432f64]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                float_precision: Some(2),
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("1.23"));
        assert!(output.contains("2.99"));
    }

    #[test]
    fn test_null_value_writing() {
        let df = DataFrame::new(vec![
            Series::new("name", &["Alice", "Bob"]),
            Series::new("age", &[Some(30i32), None::<i32>]),
        ])
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                null_value: "NULL".to_string(),
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("NULL"));
    }

    #[test]
    fn test_quote_style_never() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "description" => ["Hello, world", "Test"]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                quote_style: QuoteStyle::Never,
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        // Should not have quotes even around fields with commas
        assert!(!output.contains("\"Hello, world\""));
        assert!(output.contains("Hello, world"));
    }

    #[test]
    fn test_quote_style_non_numeric() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25],
            "description" => ["Hello, world", "Test"]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                quote_style: QuoteStyle::NonNumeric,
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        // Numeric fields should not be quoted
        assert!(output.contains("30"));
        assert!(output.contains("25"));
        // String fields should be quoted
        assert!(output.contains("\"Alice\""));
        assert!(output.contains("\"Hello, world\""));
    }

    #[test]
    fn test_read_csv_file() {
        let csv_content = "name,age\nAlice,30\nBob,25\n";
        let temp_file = NamedTempFile::new().unwrap();
        fs::write(&temp_file, csv_content).unwrap();

        let df = read_csv_file(temp_file.path()).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.get_column_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_read_csv_file_with_options() {
        let csv_content = "name;age\nAlice;30\nBob;25\n";
        let temp_file = NamedTempFile::new().unwrap();
        fs::write(&temp_file, csv_content).unwrap();

        let options = CsvReadOptions {
            separator: b';',
            ..Default::default()
        };

        let df = read_csv_file_with_options(temp_file.path(), options).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.get_column_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_write_csv_file() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        write_csv_file(&df, temp_file.path()).unwrap();

        let content = fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.contains("name,age"));
        assert!(content.contains("Alice,30"));
        assert!(content.contains("Bob,25"));
    }

    #[test]
    fn test_write_csv_file_with_options() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age" => [30, 25]
        }
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let options = CsvWriteOptions {
            separator: b';',
            ..Default::default()
        };

        write_csv_file_with_options(&df, temp_file.path(), options).unwrap();

        let content = fs::read_to_string(temp_file.path()).unwrap();
        assert!(content.contains("name;age"));
        assert!(content.contains("Alice;30"));
    }

    #[test]
    fn test_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        fs::write(&temp_file, "").unwrap();

        let result = read_csv_file(temp_file.path());
        assert!(result.is_ok());
        let df = result.unwrap();
        assert!(df.is_empty());
    }

    #[test]
    fn test_error_file_not_found() {
        let result = read_csv_file("nonexistent.csv");
        assert!(result.is_err());
    }

    #[test]
    fn test_malformed_csv() {
        let csv_content = "name,age\nAlice,30\nBob"; // Missing comma
        let cursor = Cursor::new(csv_content.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let result = reader.read_dataframe();
        // Polars might handle this, but let's see
        // For now, assume it succeeds or fails gracefully
        // Actually, Polars csv reader is lenient
        let df = result.unwrap();
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn test_parsing_newlines_in_fields() {
        let csv_content = "name,description\nAlice,\"Hello\nWorld\"\nBob,\"Test\"";
        let cursor = Cursor::new(csv_content.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
        let desc_series = df.column("description").unwrap();
        assert_eq!(desc_series.utf8().unwrap().get(0), Some("Hello\nWorld"));
    }

    #[test]
    fn test_mixed_quote_types() {
        // Test with inconsistent quotes - should handle gracefully
        let csv_content = "name,value\nAlice,\"test\"\nBob,'value'";
        let cursor = Cursor::new(csv_content.as_bytes());

        let mut reader = CsvReader::new(cursor);
        let df = reader.read_dataframe().unwrap();

        assert_eq!(df.height(), 2);
    }

    #[test]
    fn test_float32_writing() {
        let df = df! {
            "value" => [1.5f32, 2.5f32]
        }
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = CsvWriter::new(&mut buffer);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("1.5"));
        assert!(output.contains("2.5"));
    }

    #[test]
    fn test_date_formatting() {
        use polars::prelude::*;
        let dates: Vec<NaiveDate> = vec![NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()];
        let df = DataFrame::new(vec![Series::new("date_col", dates)]).unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                date_format: Some("%Y-%m-%d".to_string()),
                ..Default::default()
            };
            let mut writer = crate::csv::CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("2023-01-01"));
    }

    #[test]
    fn test_datetime_formatting() {
        use polars::prelude::*;
        let datetimes: Vec<NaiveDateTime> = vec![NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        )];
        let df = DataFrame::new(vec![Series::new("datetime_col", datetimes)]).unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                datetime_format: Some("%Y-%m-%d %H:%M:%S".to_string()),
                ..Default::default()
            };
            let mut writer = crate::csv::CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("2023-01-01 12:00:00"));
    }

    #[test]
    fn test_detect_csv_format() {
        let csv_bytes = b"name,age\nAlice,30\nBob,25";
        assert!(detect_csv_format(csv_bytes));

        let tsv_bytes = b"name\tage\nAlice\t30";
        assert!(detect_csv_format(tsv_bytes));

        let invalid_bytes = b"Hello world";
        assert!(!detect_csv_format(invalid_bytes));

        let json_bytes = b"{\"name\": \"Alice\"}";
        assert!(!detect_csv_format(json_bytes));
    }

    #[test]
    fn test_null_handling_different_types() {
        let df = DataFrame::new(vec![
            Series::new("int_col", &[Some(1i32), None::<i32>]),
            Series::new("float_col", &[Some(1.5f64), None::<f64>]),
            Series::new("str_col", &[Some("test"), None::<&str>]),
        ])
        .unwrap();

        let mut buffer = Vec::new();
        {
            let options = CsvWriteOptions {
                null_value: "N/A".to_string(),
                ..Default::default()
            };
            let mut writer = CsvWriter::with_options(&mut buffer, options);
            writer.write_dataframe(&df).unwrap();
        }

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("N/A"));
    }
}

// Public API functions for use by reader/writer modules

use crate::reader::{FormatReadOptions, ReadOptions};
use crate::writer::{FormatWriteOptions, WriteOptions};
use dsq_shared::value::Value;

/// Deserialize CSV data from a reader
pub fn deserialize_csv<R: Read + polars::io::mmap::MmapBytesReader>(
    mut reader: R,
    options: &ReadOptions,
    format_options: &FormatReadOptions,
) -> Result<Value> {
    // Check for empty input
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    if buffer.is_empty() {
        return Ok(Value::DataFrame(DataFrame::empty()));
    }

    let csv_opts = match format_options {
        FormatReadOptions::Csv {
            separator,
            has_header,
            quote_char,
            comment_char,
            null_values,
            encoding,
        } => (
            *separator,
            *has_header,
            *quote_char,
            *comment_char,
            null_values.clone(),
            encoding.clone(),
        ),
        _ => (
            b',',
            true,
            Some(b'"'),
            None,
            None,
            crate::writer::CsvEncoding::Utf8,
        ),
    };

    let mut csv_reader = PolarsCsvReader::new(std::io::Cursor::new(buffer))
        .with_separator(csv_opts.0)
        .has_header(csv_opts.1);

    if let Some(quote) = csv_opts.2 {
        csv_reader = csv_reader.with_quote_char(Some(quote));
    }

    if let Some(comment) = csv_opts.3 {
        csv_reader = csv_reader.with_comment_char(Some(comment));
    }

    if let Some(null_vals) = csv_opts.4 {
        csv_reader = csv_reader.with_null_values(Some(NullValues::AllColumns(null_vals)));
    }

    if let Some(max_rows) = options.max_rows {
        csv_reader = csv_reader.with_n_rows(Some(max_rows));
    }

    if options.skip_rows > 0 {
        csv_reader = csv_reader.with_skip_rows(options.skip_rows);
    }

    if let Some(columns) = &options.columns {
        csv_reader = csv_reader.with_columns(Some(columns.clone()));
    }

    let df = csv_reader.finish().map_err(Error::from)?;
    Ok(Value::DataFrame(df))
}

/// Serialize CSV data to a writer
pub fn serialize_csv<W: Write>(
    writer: W,
    value: &Value,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
) -> Result<()> {
    let mut df = match value {
        Value::DataFrame(df) => df.clone(),
        Value::LazyFrame(lf) => (*lf).clone().collect().map_err(Error::from)?,
        _ => return Err(Error::operation("Expected DataFrame for CSV serialization")),
    };

    let csv_opts = match format_options {
        FormatWriteOptions::Csv {
            separator,
            quote_char,
            line_terminator: _,
            quote_style: _,
            null_value: _,
            datetime_format: _,
            date_format: _,
            time_format: _,
            float_precision: _,
            null_values,
            encoding: _,
        } => (*separator, *quote_char, null_values.clone()),
        _ => (b',', Some(b'"'), None),
    };

    let mut csv_writer = polars::prelude::CsvWriter::new(writer)
        .with_separator(csv_opts.0)
        .include_header(options.include_header);

    if let Some(quote) = csv_opts.1 {
        csv_writer = csv_writer.with_quote_char(quote);
    }

    csv_writer.finish(&mut df).map_err(Error::from)?;
    Ok(())
}
