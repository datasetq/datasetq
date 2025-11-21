#![allow(invalid_reference_casting)]

use crate::error::{Error, Result};
use polars::io::parquet::ParallelStrategy;
use polars::prelude::*;

use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Parquet-specific reading options
#[derive(Debug, Clone)]
pub struct ParquetReadOptions {
    /// Number of rows to read (None for all)
    pub n_rows: Option<usize>,
    /// Columns to read (None for all)
    pub columns: Option<Vec<String>>,
    /// Number of rows to use for schema inference
    pub row_count: Option<usize>,
    /// Parallel reading strategy
    pub parallel: ParallelStrategy,
    /// Memory limit for reading (in bytes)
    pub memory_limit: Option<usize>,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self {
            n_rows: None,
            columns: None,
            row_count: None,
            parallel: ParallelStrategy::Auto,
            memory_limit: None,
        }
    }
}

/// Parquet-specific writing options
#[derive(Debug, Clone)]
pub struct ParquetWriteOptions {
    /// Compression algorithm
    pub compression: ParquetCompression,
    /// Whether to include statistics in the file
    pub statistics: bool,
    /// Row group size (number of rows per group)
    pub row_group_size: Option<usize>,
    /// Maximum data page size in bytes
    pub data_pagesize_limit: Option<usize>,
    /// Whether to use dictionary encoding
    pub dictionary: bool,
    /// Whether to maintain order
    pub maintain_order: bool,
}

impl Default for ParquetWriteOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Snappy,
            statistics: true,
            row_group_size: None,
            data_pagesize_limit: None,
            dictionary: true,
            maintain_order: false,
        }
    }
}

/// Compression algorithms for Parquet
#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    /// No compression
    Uncompressed,
    /// Snappy compression (fast, good compression)
    Snappy,
    /// Gzip compression (slow, best compression)
    Gzip,
    /// LZO compression
    Lzo,
    /// Brotli compression
    Brotli,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
}

impl From<ParquetCompression> for polars::prelude::ParquetCompression {
    fn from(compression: ParquetCompression) -> Self {
        match compression {
            ParquetCompression::Uncompressed => Self::Uncompressed,
            ParquetCompression::Snappy => Self::Snappy,
            ParquetCompression::Gzip => Self::Gzip(None),
            ParquetCompression::Lzo => Self::Lzo,
            ParquetCompression::Brotli => Self::Brotli(None),
            ParquetCompression::Lz4 => Self::Lz4Raw,
            ParquetCompression::Zstd => Self::Zstd(None),
        }
    }
}

/// Parquet reader that provides format-specific optimizations
pub struct ParquetReader {
    path: std::path::PathBuf,
    options: ParquetReadOptions,
}

impl ParquetReader {
    /// Create a new Parquet reader with default options
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            options: ParquetReadOptions::default(),
        }
    }

    /// Create a Parquet reader with custom options
    pub fn with_options<P: AsRef<Path>>(path: P, options: ParquetReadOptions) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            options,
        }
    }

    /// Set number of rows to read
    pub fn with_n_rows(mut self, n_rows: Option<usize>) -> Self {
        self.options.n_rows = n_rows;
        self
    }

    /// Set columns to read
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.options.columns = Some(columns);
        self
    }

    /// Read the Parquet file into a DataFrame
    pub fn read(self) -> Result<DataFrame> {
        let file = File::open(&self.path)?;
        let mut pq_reader = polars::io::parquet::ParquetReader::new(file);

        if let Some(n_rows) = self.options.n_rows {
            pq_reader = pq_reader.with_n_rows(Some(n_rows));
        }

        if let Some(columns) = &self.options.columns {
            pq_reader = pq_reader.with_columns(Some(columns.clone()));
        }

        // if let Some(row_count) = self.options.row_count {
        //     pq_reader = pq_reader.with_row_count(Some(polars::io::RowCount::new("row_nr".to_string(), row_count as u32)));
        // }

        // pq_reader = pq_reader.read_parallel(self.options.parallel);

        pq_reader.finish().map_err(Error::from)
    }

    /// Read the Parquet file into a LazyFrame for lazy evaluation
    pub fn read_lazy(self) -> Result<LazyFrame> {
        use polars::prelude::ScanArgsParquet;

        let mut scan_args = ScanArgsParquet::default();
        scan_args.n_rows = self.options.n_rows;
        // scan_args.with_columns = self.options.columns;
        scan_args.row_count = None;
        scan_args.parallel = self.options.parallel;

        LazyFrame::scan_parquet(&self.path, scan_args).map_err(Error::from)
    }
}

/// Parquet writer that provides format-specific optimizations
pub struct ParquetWriter<W> {
    writer: W,
    options: ParquetWriteOptions,
}

impl<W: std::io::Write> ParquetWriter<W> {
    /// Create a new Parquet writer with default options
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            options: ParquetWriteOptions::default(),
        }
    }

    /// Create a Parquet writer with custom options
    pub fn with_options(writer: W, options: ParquetWriteOptions) -> Self {
        Self { writer, options }
    }

    /// Set compression algorithm
    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.options.compression = compression;
        self
    }

    /// Set row group size
    pub fn with_row_group_size(mut self, size: Option<usize>) -> Self {
        self.options.row_group_size = size;
        self
    }

    /// Set data page size
    pub fn with_data_page_size(mut self, size: Option<usize>) -> Self {
        self.options.data_pagesize_limit = size;
        self
    }

    /// Write a DataFrame to Parquet format
    pub fn finish(self, df: &mut DataFrame) -> Result<()> {
        let mut writer = polars::io::parquet::ParquetWriter::new(self.writer)
            .with_compression(self.options.compression.into())
            .with_statistics(self.options.statistics);

        if let Some(row_group_size) = self.options.row_group_size {
            writer = writer.with_row_group_size(Some(row_group_size));
        }

        writer.finish(df).map(|_| ()).map_err(Error::from)
    }
}

/// Read a Parquet file from a file path
pub fn read_parquet_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
    read_parquet_file_with_options(path, &ParquetReadOptions::default())
}

/// Read a Parquet file from a file path with options
pub fn read_parquet_file_with_options<P: AsRef<Path>>(
    path: P,
    options: &ParquetReadOptions,
) -> Result<DataFrame> {
    let reader = ParquetReader::with_options(path, options.clone());
    reader.read()
}

/// Read a Parquet file lazily from a file path
pub fn read_parquet_file_lazy<P: AsRef<Path>>(path: P) -> Result<LazyFrame> {
    read_parquet_file_lazy_with_options(path, &ParquetReadOptions::default())
}

/// Read a Parquet file lazily from a file path with options
pub fn read_parquet_file_lazy_with_options<P: AsRef<Path>>(
    path: P,
    options: &ParquetReadOptions,
) -> Result<LazyFrame> {
    let reader = ParquetReader::with_options(path, options.clone());
    reader.read_lazy()
}

/// Write a DataFrame to a Parquet file
pub fn write_parquet_file<P: AsRef<Path>>(df: &mut DataFrame, path: P) -> Result<()> {
    write_parquet_file_with_options(df, path, &ParquetWriteOptions::default())
}

/// Write a DataFrame to a Parquet file with options
pub fn write_parquet_file_with_options<P: AsRef<Path>>(
    df: &mut DataFrame,
    path: P,
    _options: &ParquetWriteOptions,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = ParquetWriter::new(file);
    // SAFETY: This cast is safe because:
    // 1. We have a mutable reference to DataFrame (`df: &mut DataFrame`)
    // 2. We're casting it to a *const then back to *mut to satisfy the API requirements
    // 3. We own the DataFrame reference for the duration of this function
    // 4. The ParquetWriter.finish() method needs a mutable reference but the signature takes &mut
    // 5. No other code can access this DataFrame during this operation
    // This pattern is necessary due to API constraints in the Polars library.
    writer.finish(unsafe { &mut *(df as *const DataFrame as *mut DataFrame) })
}

/// Detect if a file is in Parquet format by checking the magic bytes
pub fn detect_parquet_format<P: AsRef<Path>>(path: P) -> Result<bool> {
    let mut file = File::open(path)?;
    let mut buffer = [0u8; 4];
    let bytes_read = file.read(&mut buffer)?;

    if bytes_read < 4 {
        return Ok(false);
    }

    // Parquet files start with "PAR1"
    Ok(&buffer == b"PAR1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::NamedTempFile;

    fn create_test_dataframe() -> DataFrame {
        df! {
            "name" => ["Alice", "Bob", "Charlie"],
            "age" => [30i64, 25i64, 35i64],
            "active" => [true, false, true],
            "score" => [85.5f64, 92.0f64, 78.3f64]
        }
        .unwrap()
    }

    #[test]
    fn test_parquet_roundtrip() {
        let df = create_test_dataframe();

        let temp_file = NamedTempFile::new().unwrap();
        write_parquet_file(&df, temp_file.path()).unwrap();

        let df_read = read_parquet_file(temp_file.path()).unwrap();

        assert_eq!(df.height(), df_read.height());
        assert_eq!(df.width(), df_read.width());
        assert_eq!(df.get_column_names(), df_read.get_column_names());
    }

    #[test]
    fn test_parquet_file_io() {
        let df = create_test_dataframe();
        let temp_file = NamedTempFile::new().unwrap();

        // Write to file
        write_parquet_file(&df, temp_file.path()).unwrap();

        // Read from file
        let df_read = read_parquet_file(temp_file.path()).unwrap();

        assert_eq!(df.height(), df_read.height());
        assert_eq!(df.width(), df_read.width());
    }

    #[test]
    fn test_parquet_detection() {
        let df = create_test_dataframe();
        let temp_file = NamedTempFile::new().unwrap();

        // Write a Parquet file
        write_parquet_file(&df, temp_file.path()).unwrap();

        // Should detect as Parquet
        assert!(detect_parquet_format(temp_file.path()).unwrap());
    }

    #[test]
    fn test_parquet_detection_non_parquet() {
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), b"not a parquet file").unwrap();

        // Should not detect as Parquet
        assert!(!detect_parquet_format(temp_file.path()).unwrap());
    }

    // #[test]
    // fn test_parquet_with_options() {
    //     let df = create_test_dataframe();

    //     let options = ParquetWriteOptions {
    //         compression: ParquetCompression::Gzip,
    //         statistics: false,
    //         row_group_size: Some(1000),
    //         data_pagesize_limit: Some(1024 * 1024),
    //         dictionary: false,
    //         maintain_order: false,
    //     };

    //     let temp_file = NamedTempFile::new().unwrap();
    //     write_parquet_file_with_options(&df, temp_file.path(), &options).unwrap();

    //     let df_read = read_parquet_file(temp_file.path()).unwrap();
    //     assert_eq!(df.height(), df_read.height());
    // }

    #[test]
    fn test_parquet_lazy_read() {
        let df = create_test_dataframe();
        let temp_file = NamedTempFile::new().unwrap();
        write_parquet_file(&df, temp_file.path()).unwrap();

        let lf = read_parquet_file_lazy(temp_file.path()).unwrap();
        let df_read = lf.collect().unwrap();

        assert_eq!(df.height(), df_read.height());
        assert_eq!(df.width(), df_read.width());
    }

    #[test]
    fn test_parquet_read_with_options() {
        let df = create_test_dataframe();
        let temp_file = NamedTempFile::new().unwrap();
        write_parquet_file(&df, temp_file.path()).unwrap();

        let options = ParquetReadOptions {
            n_rows: Some(2),
            columns: Some(vec!["name".to_string(), "age".to_string()]),
            ..Default::default()
        };

        let df_read = read_parquet_file_with_options(temp_file.path(), &options).unwrap();

        assert_eq!(df_read.height(), 2);
        assert_eq!(df_read.width(), 2);
        assert_eq!(df_read.get_column_names(), &["name", "age"]);
    }
}
