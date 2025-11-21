use crate::{Error, Result};
use polars::prelude::*;
use std::fs::File;

use super::super::options::{DsAvroCompression, FormatWriteOptions, WriteOptions};

/// Write Avro file
pub fn write_avro(
    _df: &mut DataFrame,
    _options: &WriteOptions,
    _format_options: &FormatWriteOptions,
    _path: &str,
) -> Result<()> {
    // TODO: Implement Avro writing
    Err(Error::Other("Avro writing not yet implemented".to_string()))
}
