use crate::{Error, Result};
use polars::prelude::*;
use std::fs::File;

use super::super::options::{DsParquetCompression, FormatWriteOptions, WriteOptions};

/// Write Parquet file
pub fn write_parquet(
    df: &mut DataFrame,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
    path: &str,
) -> Result<()> {
    let parquet_opts = match format_options {
        FormatWriteOptions::Parquet {
            compression,
            statistics,
            row_group_size,
            data_pagesize_limit,
        } => (
            compression.clone(),
            *statistics,
            *row_group_size,
            *data_pagesize_limit,
        ),
        _ => (DsParquetCompression::Snappy, true, None, None),
    };

    let file = File::create(path)?;
    let compression = match parquet_opts.0 {
        DsParquetCompression::Uncompressed => ParquetCompression::Uncompressed,
        DsParquetCompression::Snappy => ParquetCompression::Snappy,
        DsParquetCompression::Gzip => ParquetCompression::Gzip(None),
        DsParquetCompression::Lzo => ParquetCompression::Lzo,
        DsParquetCompression::Brotli => ParquetCompression::Brotli(None),
        DsParquetCompression::Lz4 => ParquetCompression::Lzo,
        DsParquetCompression::Zstd => ParquetCompression::Zstd(None),
    };

    let mut writer = ParquetWriter::new(file).with_compression(compression);

    if let Some(row_group_size) = parquet_opts.2 {
        writer = writer.with_row_group_size(Some(row_group_size));
    }

    if let Some(page_size) = parquet_opts.3 {
        writer = writer.with_data_pagesize_limit(Some(page_size));
    }

    writer.finish(df).map_err(Error::from)?;
    Ok(())
}
