use crate::{Error, Result};
use polars::prelude::*;
use std::fs::File;
use std::io::BufWriter;

use super::super::options::{DsQuoteStyle, FormatWriteOptions, WriteOptions};
use super::super::utils::series_value_to_json;

/// Write CSV file
pub fn write_csv(
    df: &mut DataFrame,
    options: &WriteOptions,
    format_options: &FormatWriteOptions,
    path: &str,
) -> Result<()> {
    let csv_opts = match format_options {
        FormatWriteOptions::Csv {
            separator,
            quote_char,
            line_terminator,
            quote_style,
            null_value,
            datetime_format,
            date_format,
            time_format,
            float_precision,
        } => (
            *separator,
            *quote_char,
            line_terminator.clone(),
            quote_style.clone(),
            null_value.clone(),
            datetime_format.clone(),
            date_format.clone(),
            time_format.clone(),
            *float_precision,
        ),
        _ => (
            b',',
            b'"',
            "\n".to_string(),
            DsQuoteStyle::Necessary,
            "".to_string(),
            None,
            None,
            None,
            None,
        ),
    };

    let file = File::create(path)?;
    let quote_style = match csv_opts.3 {
        DsQuoteStyle::Always => polars::io::csv::QuoteStyle::Always,
        DsQuoteStyle::Necessary => polars::io::csv::QuoteStyle::Necessary,
        DsQuoteStyle::NonNumeric => polars::io::csv::QuoteStyle::NonNumeric,
        DsQuoteStyle::Never => polars::io::csv::QuoteStyle::Never,
    };
    let mut writer = CsvWriter::new(file)
        .with_separator(csv_opts.0)
        .with_quote_char(csv_opts.1)
        .include_header(options.include_header)
        .with_quote_style(quote_style)
        .with_null_value(csv_opts.4);

    // TODO: Fix date/time format options
    // if let Some(date_fmt) = csv_opts.6 {
    //     writer = writer.with_date_format(date_fmt);
    // }
    // if let Some(time_fmt) = csv_opts.7 {
    //     writer = writer.with_time_format(time_fmt);
    // }
    // if let Some(datetime_fmt) = csv_opts.5 {
    //     writer = writer.with_datetime_format(datetime_fmt);
    // }
    if let Some(precision) = csv_opts.8 {
        writer = writer.with_float_precision(Some(precision));
    }

    writer.finish(df).map_err(Error::from)?;
    Ok(())
}
