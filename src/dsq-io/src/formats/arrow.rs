use crate::{Error, Result};
use polars::prelude::*;
use std::fs::File;

use super::super::options::{DsIpcCompression, FormatWriteOptions, WriteOptions};

/// Write Arrow file
pub fn write_arrow(
    df: &mut DataFrame,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
    path: &str,
) -> Result<()> {
    let arrow_opts = match format_options {
        FormatWriteOptions::Arrow { compression } => compression.clone(),
        _ => None,
    };

    let file = File::create(path)?;
    let mut writer = IpcWriter::new(file);

    if let Some(compression) = arrow_opts {
        match compression {
            DsIpcCompression::Lz4 => {
                writer = writer.with_compression(Some(IpcCompression::LZ4));
            }
            DsIpcCompression::Zstd => {
                writer = writer.with_compression(Some(IpcCompression::ZSTD));
            }
            DsIpcCompression::Uncompressed => {
                writer = writer.with_compression(None);
            }
        }
    }

    writer.finish(df).map_err(Error::from)?;
    Ok(())
}
