use crate::error::{Error, Result};
use crate::format::DataFormat;
use crate::reader::options::{FormatReadOptions, ReadOptions};
use dsq_shared::value::Value;
use polars::prelude::*;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

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
            _ => (
                b',',
                true,
                Some(b'"'),
                None,
                None,
                crate::csv::CsvEncoding::Utf8,
            ),
        };

        let file = std::fs::File::open(&self.path).map_err(Error::from)?;
        let buf_reader = std::io::BufReader::with_capacity(128 * 1024, file); // 128KB buffer
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
    #[cfg(feature = "parquet")]
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

        let file = std::fs::File::open(&self.path)?;
        let mut reader = std::io::BufReader::with_capacity(128 * 1024, file); // 128KB buffer

        if json_opts.0 {
            // JSON Lines format
            self.read_json_lines(&mut reader, options, json_opts.1)
        } else {
            // Regular JSON format
            self.read_json_regular(&mut reader, options)
        }
    }

    /// Read JSON5 file
    fn read_json5(&self, options: &ReadOptions) -> Result<Value> {
        let json5_opts = match &self.format_options {
            FormatReadOptions::Json5 {
                lines,
                ignore_errors,
            } => (*lines, *ignore_errors),
            _ => (false, false),
        };

        let file = std::fs::File::open(&self.path)?;
        let mut reader = std::io::BufReader::with_capacity(128 * 1024, file); // 128KB buffer

        if json5_opts.0 {
            // JSON5 Lines format
            self.read_json5_lines(&mut reader, options, json5_opts.1)
        } else {
            // Regular JSON5 format
            self.read_json5_regular(&mut reader, options)
        }
    }

    /// Read JSON Lines format
    fn read_json_lines<R: std::io::BufRead>(
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
                            return Err(Error::Format(
                                crate::error::FormatError::SerializationError(format!(
                                    "Invalid JSON on line {}: {}",
                                    count + skip_rows + 1,
                                    e
                                )),
                            ));
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
    fn read_json_regular<R: std::io::Read>(
        &self,
        reader: &mut R,
        options: &ReadOptions,
    ) -> Result<Value> {
        let json_val: serde_json::Value = serde_json::from_reader(reader).map_err(|e| {
            Error::Format(crate::error::FormatError::SerializationError(format!(
                "Invalid JSON: {}",
                e
            )))
        })?;

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

    /// Read JSON5 Lines format
    fn read_json5_lines<R: std::io::BufRead>(
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
                match json5::from_str::<serde_json::Value>(trimmed) {
                    Ok(json_val) => {
                        rows.push(Value::from_json(json_val));
                        count += 1;
                    }
                    Err(e) => {
                        if !ignore_errors {
                            return Err(Error::Format(
                                crate::error::FormatError::SerializationError(format!(
                                    "Invalid JSON5 on line {}: {}",
                                    count + skip_rows + 1,
                                    e
                                )),
                            ));
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

    /// Read regular JSON5 format
    fn read_json5_regular<R: std::io::Read>(
        &self,
        reader: &mut R,
        options: &ReadOptions,
    ) -> Result<Value> {
        let mut content = String::new();
        reader.read_to_string(&mut content).map_err(Error::from)?;
        let json_val: serde_json::Value = json5::from_str(&content).map_err(|e| {
            Error::Format(crate::error::FormatError::SerializationError(format!(
                "Invalid JSON5: {}",
                e
            )))
        })?;

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

        let file = std::fs::File::open(&self.path)?;
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
            Err(Error::Format(
                crate::error::FormatError::UnsupportedFeature(
                    "Avro schema must be a record".to_string(),
                ),
            ))
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
            Err(Error::Format(
                crate::error::FormatError::UnsupportedFeature(
                    "Avro schema must be a record".to_string(),
                ),
            ))
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

        let file = std::fs::File::open(&self.path)?;
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

impl crate::reader::data_reader::DataReader for FileReader {
    fn read(&mut self, options: &ReadOptions) -> Result<Value> {
        if !self.format.supports_reading() {
            return Err(Error::Format(
                crate::error::FormatError::UnsupportedFeature(format!(
                    "{} format does not support reading",
                    self.format
                )),
            ));
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
            #[cfg(feature = "parquet")]
            DataFormat::Parquet => self.read_parquet(options),
            #[cfg(not(feature = "parquet"))]
            DataFormat::Parquet => Err(Error::Format(
                crate::error::FormatError::UnsupportedFeature(
                    "Parquet not supported in this build".to_string(),
                ),
            )),
            DataFormat::Json | DataFormat::JsonLines | DataFormat::JsonCompact => {
                // Adjust for JSON Lines
                if self.format == DataFormat::JsonLines {
                    if let FormatReadOptions::Json { lines, .. } = &mut self.format_options {
                        *lines = true;
                    }
                }
                self.read_json(options)
            }
            DataFormat::Json5 => self.read_json5(options),
            DataFormat::Avro => self.read_avro(options),
            DataFormat::Arrow => self.read_arrow(options),
            DataFormat::Excel | DataFormat::Orc => Err(Error::Format(
                crate::error::FormatError::UnsupportedFeature(format!(
                    "{} format does not support reading",
                    self.format
                )),
            )),
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
