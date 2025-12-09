use crate::{Error, Result};
use dsq_formats::format::DataFormat;
use dsq_shared::value::Value;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// Options for reading data files
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Maximum number of rows to read (None for all)
    pub max_rows: Option<usize>,
    /// Whether to infer schema from data
    pub infer_schema: bool,
    /// Number of rows to use for schema inference
    pub infer_schema_length: Option<usize>,
    /// Whether to use lazy evaluation
    pub lazy: bool,
    /// Custom schema to apply
    pub schema: Option<Schema>,
    /// Skip first N rows
    pub skip_rows: usize,
    /// Column names to select (None for all)
    pub columns: Option<Vec<String>>,
    /// Whether to parse dates
    pub parse_dates: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_rows: None,
            infer_schema: true,
            infer_schema_length: Some(1000),
            lazy: false,
            schema: None,
            skip_rows: 0,
            columns: None,
            parse_dates: true,
        }
    }
}

/// Format-specific read options
#[derive(Debug, Clone)]
pub enum FormatReadOptions {
    Csv {
        separator: u8,
        has_header: bool,
        quote_char: Option<u8>,
        comment_char: Option<u8>,
        null_values: Option<Vec<String>>,
        encoding: CsvEncoding,
    },
    Parquet {
        parallel: bool,
        use_statistics: bool,
        columns: Option<Vec<String>>,
    },
    Json {
        lines: bool,
        ignore_errors: bool,
    },
    Avro {
        columns: Option<Vec<String>>,
    },
    Arrow {
        columns: Option<Vec<String>>,
    },
}

#[derive(Debug, Clone)]
pub enum CsvEncoding {
    Utf8,
    Utf8Lossy,
}

impl Default for FormatReadOptions {
    fn default() -> Self {
        FormatReadOptions::Csv {
            separator: b',',
            has_header: true,
            quote_char: Some(b'"'),
            comment_char: None,
            null_values: None,
            encoding: CsvEncoding::Utf8,
        }
    }
}

/// Trait for reading data from various sources
pub trait DataReader {
    /// Read data into a DataFrame
    fn read(&mut self, options: &ReadOptions) -> Result<Value>;

    /// Read data into a LazyFrame (if supported)
    fn read_lazy(&mut self, options: &ReadOptions) -> Result<LazyFrame> {
        // Default implementation: read eagerly then convert to lazy
        match self.read(options)? {
            Value::DataFrame(df) => Ok(df.lazy()),
            Value::LazyFrame(lf) => Ok(*lf),
            _ => Err(Error::operation(
                "Expected DataFrame or LazyFrame from reader",
            )),
        }
    }

    /// Check if the reader supports lazy evaluation
    fn supports_lazy(&self) -> bool {
        false
    }

    /// Get the detected or specified format
    fn format(&self) -> DataFormat;

    /// Peek at the first few rows without consuming the reader
    fn peek(&mut self, rows: usize) -> Result<DataFrame> {
        let mut options = ReadOptions::default();
        options.max_rows = Some(rows);
        match self.read(&options)? {
            Value::DataFrame(df) => Ok(df),
            Value::LazyFrame(lf) => lf.collect().map_err(Error::from),
            _ => Err(Error::operation("Expected DataFrame from peek")),
        }
    }
}

/// File-based data reader
pub struct FileReader {
    path: String,
    format: DataFormat,
    format_options: FormatReadOptions,
}

impl FileReader {
    /// Create a new file reader with automatic format detection
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let format = DataFormat::from_path(path_ref)?;

        Ok(Self {
            path: path_ref.to_string_lossy().to_string(),
            format,
            format_options: FormatReadOptions::default(),
        })
    }

    /// Create a new file reader with explicit format
    pub fn with_format<P: AsRef<Path>>(path: P, format: DataFormat) -> Self {
        Self {
            path: path.as_ref().to_string_lossy().to_string(),
            format,
            format_options: FormatReadOptions::default(),
        }
    }

    /// Set format-specific options
    pub fn with_format_options(mut self, options: FormatReadOptions) -> Self {
        self.format_options = options;
        self
    }

    /// Read CSV file
    fn read_csv(&self, options: &ReadOptions) -> Result<Value> {
        let csv_opts = match &self.format_options {
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
            _ => (b',', true, Some(b'"'), None, None, CsvEncoding::Utf8),
        };

        let file = File::open(&self.path).map_err(Error::from)?;
        let buf_reader = BufReader::with_capacity(128 * 1024, file); // 128KB buffer
        let mut reader = CsvReader::new(buf_reader)
            .with_separator(csv_opts.0)
            .has_header(csv_opts.1);

        if let Some(quote) = csv_opts.2 {
            reader = reader.with_quote_char(Some(quote));
        }

        if let Some(comment) = csv_opts.3 {
            reader = reader.with_comment_char(Some(comment));
        }

        if let Some(null_vals) = csv_opts.4 {
            reader = reader.with_null_values(Some(NullValues::AllColumns(null_vals)));
        }

        if let Some(max_rows) = options.max_rows {
            reader = reader.with_n_rows(Some(max_rows));
        }

        if let Some(schema) = &options.schema {
            reader = reader.with_schema(Some(std::sync::Arc::new(schema.clone())));
        } else if options.infer_schema {
            if let Some(infer_len) = options.infer_schema_length {
                reader = reader.infer_schema(Some(infer_len));
            }
        }

        if options.skip_rows > 0 {
            reader = reader.with_skip_rows(options.skip_rows);
        }

        if let Some(cols) = &options.columns {
            reader = reader.with_columns(Some(cols.clone()));
        }

        let df = reader.finish().map_err(Error::from)?;
        if options.lazy {
            Ok(Value::LazyFrame(Box::new(df.lazy())))
        } else {
            Ok(Value::DataFrame(df))
        }
    }

    /// Read Parquet file
    fn read_parquet(&self, options: &ReadOptions) -> Result<Value> {
        let parquet_opts = match &self.format_options {
            FormatReadOptions::Parquet {
                parallel,
                use_statistics,
                columns,
            } => (*parallel, *use_statistics, columns.clone()),
            _ => (true, true, None),
        };

        let mut reader =
            LazyFrame::scan_parquet(&self.path, ScanArgsParquet::default()).map_err(Error::from)?;

        if let Some(cols) = parquet_opts.2.or_else(|| options.columns.clone()) {
            reader = reader.select(cols.iter().map(|s| col(s)).collect::<Vec<_>>());
        }

        if let Some(max_rows) = options.max_rows {
            reader = reader.limit(max_rows as u32);
        }

        if options.skip_rows > 0 {
            reader = reader.slice(options.skip_rows as i64, u32::MAX);
        }

        if options.lazy {
            Ok(Value::LazyFrame(Box::new(reader)))
        } else {
            let df = reader.collect().map_err(Error::from)?;
            Ok(Value::DataFrame(df))
        }
    }

    /// Read JSON file
    fn read_json(&self, options: &ReadOptions) -> Result<Value> {
        let json_opts = match &self.format_options {
            FormatReadOptions::Json {
                lines,
                ignore_errors,
            } => (*lines, *ignore_errors),
            _ => (false, false),
        };

        let file = File::open(&self.path)?;
        let mut reader = BufReader::with_capacity(128 * 1024, file); // 128KB buffer

        if json_opts.0 {
            // JSON Lines format
            self.read_json_lines(&mut reader, options, json_opts.1)
        } else {
            // Regular JSON format
            self.read_json_regular(&mut reader, options)
        }
    }

    /// Read JSON Lines format
    fn read_json_lines<R: BufRead>(
        &self,
        reader: &mut R,
        options: &ReadOptions,
        ignore_errors: bool,
    ) -> Result<Value> {
        let mut rows = Vec::new();
        let mut line = String::new();
        let mut count = 0;

        let max_rows = options.max_rows.unwrap_or(usize::MAX);
        let skip_rows = options.skip_rows;
        let mut skipped = 0;

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break; // EOF
            }

            if skipped < skip_rows {
                skipped += 1;
                continue;
            }

            if count >= max_rows {
                break;
            }

            let trimmed = line.trim();
            if !trimmed.is_empty() {
                match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(json_val) => {
                        rows.push(Value::from_json(json_val));
                        count += 1;
                    }
                    Err(e) => {
                        if !ignore_errors {
                            return Err(Error::Format(format!(
                                "Invalid JSON on line {}: {}",
                                count + skip_rows + 1,
                                e
                            )));
                        }
                    }
                }
            }
        }

        let array_value = Value::Array(rows);
        let df = array_value.to_dataframe()?;

        if options.lazy {
            Ok(Value::LazyFrame(Box::new(df.lazy())))
        } else {
            Ok(Value::DataFrame(df))
        }
    }

    /// Read regular JSON format
    fn read_json_regular<R: Read>(&self, reader: &mut R, options: &ReadOptions) -> Result<Value> {
        let json_val: serde_json::Value = serde_json::from_reader(reader)
            .map_err(|e| Error::Format(format!("Invalid JSON: {}", e)))?;

        let value = Value::from_json(json_val);
        let mut df = value.to_dataframe()?;

        // Apply options
        if options.skip_rows > 0 {
            df = df.slice(options.skip_rows as i64, usize::MAX);
        }

        if let Some(max_rows) = options.max_rows {
            df = df.head(Some(max_rows));
        }

        if let Some(cols) = &options.columns {
            df = df.select(cols).map_err(Error::from)?;
        }
        if options.lazy {
            Ok(Value::LazyFrame(Box::new(df.lazy())))
        } else {
            Ok(Value::DataFrame(df))
        }
    }



    /// Read Avro file
    fn read_avro(&self, options: &ReadOptions) -> Result<Value> {
        use apache_avro::Reader;

        let file = File::open(&self.path)?;
        let reader = Reader::new(file)?;

        let schema = reader.writer_schema().clone();
        let mut records = Vec::new();

        for result in reader {
            let record = result?;
            records.push(record);
        }

        if records.is_empty() {
            // Return empty DataFrame with schema inferred from Avro schema
            let empty_df = self.avro_schema_to_empty_dataframe(&schema)?;
            return if options.lazy {
                Ok(Value::LazyFrame(Box::new(empty_df.lazy())))
            } else {
                Ok(Value::DataFrame(empty_df))
            };
        }

        // Convert records to DataFrame
        let df = self.avro_records_to_dataframe(&records, &schema)?;

        // Apply options
        let mut df = df;
        if options.skip_rows > 0 {
            df = df.slice(options.skip_rows as i64, usize::MAX);
        }

        if let Some(max_rows) = options.max_rows {
            df = df.head(Some(max_rows));
        }

        if let Some(cols) = &options.columns {
            df = df.select(cols).map_err(Error::from)?;
        }

        if options.lazy {
            Ok(Value::LazyFrame(Box::new(df.lazy())))
        } else {
            Ok(Value::DataFrame(df))
        }
    }

    /// Convert Avro schema to empty Polars DataFrame
    fn avro_schema_to_empty_dataframe(&self, schema: &apache_avro::Schema) -> Result<DataFrame> {
        if let apache_avro::Schema::Record(record_schema) = schema {
            let mut columns = Vec::new();

            for field in &record_schema.fields {
                let series = match &field.schema {
                    apache_avro::Schema::String => Series::new(&field.name, Vec::<String>::new()),
                    apache_avro::Schema::Int => Series::new(&field.name, Vec::<i32>::new()),
                    apache_avro::Schema::Long => Series::new(&field.name, Vec::<i64>::new()),
                    apache_avro::Schema::Float => Series::new(&field.name, Vec::<f32>::new()),
                    apache_avro::Schema::Double => Series::new(&field.name, Vec::<f64>::new()),
                    apache_avro::Schema::Boolean => Series::new(&field.name, Vec::<bool>::new()),
                    _ => Series::new(&field.name, Vec::<String>::new()), // Default to string for complex types
                };
                columns.push(series);
            }

            DataFrame::new(columns).map_err(Error::from)
        } else {
            Err(Error::Format("Avro schema must be a record".to_string()))
        }
    }

    /// Convert Avro records to Polars DataFrame
    fn avro_records_to_dataframe(
        &self,
        records: &[apache_avro::types::Value],
        schema: &apache_avro::Schema,
    ) -> Result<DataFrame> {
        use apache_avro::types::Value as AvroValue;

        if let apache_avro::Schema::Record(record_schema) = schema {
            let mut column_data: HashMap<String, Vec<AvroValue>> = HashMap::new();

            // Initialize columns
            for field in &record_schema.fields {
                column_data.insert(field.name.clone(), Vec::new());
            }

            // Collect data from records
            for record in records {
                if let AvroValue::Record(fields) = record {
                    for (field_name, value) in fields {
                        if let Some(column) = column_data.get_mut(field_name) {
                            column.push(value.clone());
                        }
                    }
                }
            }

            // Convert to Polars Series
            let mut series_vec = Vec::new();
            for field in &record_schema.fields {
                if let Some(values) = column_data.get(&field.name) {
                    let series = self.avro_values_to_series(&field.name, values, &field.schema)?;
                    series_vec.push(series);
                }
            }

            DataFrame::new(series_vec).map_err(Error::from)
        } else {
            Err(Error::Format("Avro schema must be a record".to_string()))
        }
    }

    /// Convert Avro values to Polars Series
    fn avro_values_to_series(
        &self,
        name: &str,
        values: &[apache_avro::types::Value],
        field_schema: &apache_avro::Schema,
    ) -> Result<Series> {
        use apache_avro::types::Value as AvroValue;

        match field_schema {
            apache_avro::Schema::String => {
                let strings: Vec<String> = values
                    .iter()
                    .map(|v| match v {
                        AvroValue::String(s) => s.clone(),
                        AvroValue::Union(_, boxed_val) => {
                            if let AvroValue::String(s) = &**boxed_val {
                                s.clone()
                            } else {
                                "".to_string()
                            }
                        }
                        _ => "".to_string(),
                    })
                    .collect();
                Ok(Series::new(name, strings))
            }
            apache_avro::Schema::Int => {
                let ints: Vec<Option<i32>> = values
                    .iter()
                    .map(|v| match v {
                        AvroValue::Int(i) => Some(*i),
                        AvroValue::Union(_, boxed_val) => {
                            if let AvroValue::Int(i) = &**boxed_val {
                                Some(*i)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(name, ints))
            }
            apache_avro::Schema::Long => {
                let longs: Vec<Option<i64>> = values
                    .iter()
                    .map(|v| match v {
                        AvroValue::Long(l) => Some(*l),
                        AvroValue::Union(_, boxed_val) => {
                            if let AvroValue::Long(l) = &**boxed_val {
                                Some(*l)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(name, longs))
            }
            apache_avro::Schema::Float => {
                let floats: Vec<Option<f32>> = values
                    .iter()
                    .map(|v| match v {
                        AvroValue::Float(f) => Some(*f),
                        AvroValue::Union(_, boxed_val) => {
                            if let AvroValue::Float(f) = &**boxed_val {
                                Some(*f)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(name, floats))
            }
            apache_avro::Schema::Double => {
                let doubles: Vec<Option<f64>> = values
                    .iter()
                    .map(|v| match v {
                        AvroValue::Double(d) => Some(*d),
                        AvroValue::Union(_, boxed_val) => {
                            if let AvroValue::Double(d) = &**boxed_val {
                                Some(*d)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(name, doubles))
            }
            apache_avro::Schema::Boolean => {
                let bools: Vec<Option<bool>> = values
                    .iter()
                    .map(|v| match v {
                        AvroValue::Boolean(b) => Some(*b),
                        AvroValue::Union(_, boxed_val) => {
                            if let AvroValue::Boolean(b) = &**boxed_val {
                                Some(*b)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(name, bools))
            }
            _ => {
                // For complex types, convert to string representation
                let strings: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
                Ok(Series::new(name, strings))
            }
        }
    }

    /// Read Arrow file
    fn read_arrow(&self, options: &ReadOptions) -> Result<Value> {
        use polars::io::ipc::IpcReader;

        let file = File::open(&self.path)?;
        let mut reader = IpcReader::new(file);

        if let Some(cols) = &options.columns {
            reader = reader.with_columns(Some(cols.clone()));
        }

        if let Some(max_rows) = options.max_rows {
            reader = reader.with_n_rows(Some(max_rows));
        }

        let df = reader.finish().map_err(Error::from)?;

        if options.lazy {
            Ok(Value::LazyFrame(Box::new(df.lazy())))
        } else {
            Ok(Value::DataFrame(df))
        }
    }
}

impl DataReader for FileReader {
    fn read(&mut self, options: &ReadOptions) -> Result<Value> {
        if !self.format.supports_reading() {
            return Err(Error::Format(format!(
                "Unsupported feature: {} format does not support reading",
                self.format
            )));
        }

        match self.format {
            DataFormat::Csv | DataFormat::Tsv | DataFormat::Adt => {
                // Adjust separator for TSV
                if self.format == DataFormat::Tsv {
                    if let FormatReadOptions::Csv { separator, .. } = &mut self.format_options {
                        *separator = b'\t';
                    }
                }
                self.read_csv(options)
            }
            DataFormat::Parquet => self.read_parquet(options),
            DataFormat::Json | DataFormat::JsonLines | DataFormat::JsonCompact => {
                // Adjust for JSON Lines
                if self.format == DataFormat::JsonLines {
                    if let FormatReadOptions::Json { lines, .. } = &mut self.format_options {
                        *lines = true;
                    }
                }
                self.read_json(options)
            }
            DataFormat::Avro => self.read_avro(options),
            DataFormat::Arrow => self.read_arrow(options),
            DataFormat::Excel | DataFormat::Orc => Err(Error::Format(format!(
                "Unsupported feature: {} format does not support reading",
                self.format
            ))),
        }
    }

    fn read_lazy(&mut self, options: &ReadOptions) -> Result<LazyFrame> {
        let mut lazy_options = options.clone();
        lazy_options.lazy = true;

        match self.read(&lazy_options)? {
            Value::LazyFrame(lf) => Ok(*lf),
            Value::DataFrame(df) => Ok(df.lazy()),
            _ => Err(Error::operation("Expected DataFrame or LazyFrame")),
        }
    }

    fn supports_lazy(&self) -> bool {
        self.format.supports_lazy_reading()
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}

/// Reader for in-memory data
pub struct MemoryReader {
    data: Vec<u8>,
    format: DataFormat,
    format_options: FormatReadOptions,
}

impl MemoryReader {
    /// Create a new memory reader
    pub fn new(data: Vec<u8>, format: DataFormat) -> Self {
        Self {
            data,
            format,
            format_options: FormatReadOptions::default(),
        }
    }

    /// Set format-specific options
    pub fn with_format_options(mut self, options: FormatReadOptions) -> Self {
        self.format_options = options;
        self
    }
}

impl DataReader for MemoryReader {
    fn read(&mut self, options: &ReadOptions) -> Result<Value> {
        use std::io::Cursor;

        match self.format {
            DataFormat::Csv | DataFormat::Tsv => {
                let cursor = Cursor::new(&self.data);
                let separator = if self.format == DataFormat::Tsv {
                    b'\t'
                } else {
                    b','
                };

                let mut reader = CsvReader::new(cursor)
                    .with_separator(separator)
                    .has_header(true);

                if let Some(max_rows) = options.max_rows {
                    reader = reader.with_n_rows(Some(max_rows));
                }

                let df = reader.finish().map_err(Error::from)?;

                if options.lazy {
                    Ok(Value::LazyFrame(Box::new(df.lazy())))
                } else {
                    Ok(Value::DataFrame(df))
                }
            }
            DataFormat::Json | DataFormat::JsonLines => {
                let json_val: serde_json::Value = serde_json::from_slice(&self.data)
                    .map_err(|e| Error::Format(format!("Invalid JSON: {}", e)))?;

                let value = Value::from_json(json_val);
                let df = value.to_dataframe()?;

                if options.lazy {
                    Ok(Value::LazyFrame(Box::new(df.lazy())))
                } else {
                    Ok(Value::DataFrame(df))
                }
            }
            _ => Err(Error::Format(format!(
                "Unsupported feature: {} format not supported for memory reading",
                self.format
            ))),
        }
    }

    fn format(&self) -> DataFormat {
        self.format
    }
}

/// Create a reader from a file path with automatic format detection
pub fn from_path<P: AsRef<Path>>(path: P) -> Result<FileReader> {
    FileReader::new(path)
}

/// Create a reader from a file path with explicit format
pub fn from_path_with_format<P: AsRef<Path>>(path: P, format: DataFormat) -> FileReader {
    FileReader::with_format(path, format)
}

/// Create a reader from in-memory data
pub fn from_memory(data: Vec<u8>, format: DataFormat) -> MemoryReader {
    MemoryReader::new(data, format)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_tsv_reader() {
        let tsv_data = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tSan Francisco\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(tsv_data.as_bytes()).unwrap();

        let mut reader = FileReader::with_format(temp_file.path(), DataFormat::Tsv);
        let options = ReadOptions::default();

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 3);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_json_lines_reader() {
        let jsonl_data = r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}
{"name":"Charlie","age":35}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(jsonl_data.as_bytes()).unwrap();

        let mut reader = FileReader::with_format(temp_file.path(), DataFormat::JsonLines);
        let options = ReadOptions::default();

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 3);
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }


    #[test]
    fn test_lazy_reading() {
        let csv_data = "name,age\nAlice,30\nBob,25\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.lazy = true;

        let result = reader.read(&options).unwrap();
        match result {
            Value::LazyFrame(_) => {
                // Successfully read as lazy frame
            }
            _ => panic!("Expected LazyFrame"),
        }
    }

    #[test]
    fn test_skip_rows() {
        let csv_data = "header1,header2\nskip1,skip2\nAlice,30\nBob,25\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.skip_rows = 1;

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2); // Should skip the first data row
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_max_rows() {
        let csv_data = "name,age\nAlice,30\nBob,25\nCharlie,35\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.max_rows = Some(2);

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_column_selection() {
        let csv_data = "name,age,city\nAlice,30,NYC\nBob,25,SF\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.columns = Some(vec!["name".to_string(), "age".to_string()]);

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_memory_reader_json() {
        let json_data = r#"[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]"#;

        let mut reader = MemoryReader::new(json_data.as_bytes().to_vec(), DataFormat::Json);
        let options = ReadOptions::default();

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_peek_method() {
        let csv_data = "name,age\nAlice,30\nBob,25\nCharlie,35\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();

        let df = reader.peek(2).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_lazy_reading() {
        let csv_data = "name,age\nAlice,30\nBob,25\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.lazy = true;

        let result = reader.read(&options).unwrap();
        match result {
            Value::LazyFrame(_) => {
                // Successfully read as lazy frame
            }
            _ => panic!("Expected LazyFrame"),
        }
    }

    #[test]
    fn test_skip_rows() {
        let csv_data = "header1,header2\nskip1,skip2\nAlice,30\nBob,25\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.skip_rows = 1;

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2); // Should skip the first data row
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_max_rows() {
        let csv_data = "name,age\nAlice,30\nBob,25\nCharlie,35\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.max_rows = Some(2);

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_column_selection() {
        let csv_data = "name,age,city\nAlice,30,NYC\nBob,25,SF\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();
        let mut options = ReadOptions::default();
        options.columns = Some(vec!["name".to_string(), "age".to_string()]);

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_memory_reader_json() {
        let json_data = r#"[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]"#;

        let mut reader = MemoryReader::new(json_data.as_bytes().to_vec(), DataFormat::Json);
        let options = ReadOptions::default();

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                assert_eq!(df.width(), 2);
            }
            _ => panic!("Expected DataFrame"),
        }
    }

    #[test]
    fn test_peek_method() {
        let csv_data = "name,age\nAlice,30\nBob,25\nCharlie,35\n";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let mut reader = FileReader::new(temp_file.path()).unwrap();

        let df = reader.peek(2).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_invalid_json_error() {
        let invalid_jsonl = r#"{"name":"Alice","age":30}
invalid json line
{"name":"Bob","age":25}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(invalid_jsonl.as_bytes()).unwrap();

        let mut reader = FileReader::with_format(temp_file.path(), DataFormat::JsonLines);
        let options = ReadOptions::default();

        let result = reader.read(&options);
        assert!(result.is_err()); // Should fail on invalid JSON
    }

    #[test]
    fn test_ignore_errors_json() {
        let invalid_jsonl = r#"{"name":"Alice","age":30}
invalid json line
{"name":"Bob","age":25}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(invalid_jsonl.as_bytes()).unwrap();

        let mut reader = FileReader::with_format(temp_file.path(), DataFormat::JsonLines)
            .with_format_options(FormatReadOptions::Json {
                lines: true,
                ignore_errors: true,
            });
        let options = ReadOptions::default();

        let result = reader.read(&options).unwrap();
        match result {
            Value::DataFrame(df) => {
                assert_eq!(df.height(), 2); // Should skip invalid line
            }
            _ => panic!("Expected DataFrame"),
        }
    }
}
