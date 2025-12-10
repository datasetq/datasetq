use crate::{Error, Result};
use polars::prelude::*;
use std::fs::File;
use std::io::Write;

use super::super::options::{DsAvroCompression, FormatWriteOptions, WriteOptions};

/// Write Avro file
pub fn write_avro(
    df: &mut DataFrame,
    _options: &WriteOptions,
    format_options: &FormatWriteOptions,
    path: &str,
) -> Result<()> {
    use apache_avro::{types::Value as AvroValue, Codec, Schema, Writer};

    // Get compression codec
    let compression = match format_options {
        FormatWriteOptions::Avro { compression } => compression.clone(),
        _ => DsAvroCompression::Uncompressed,
    };

    let codec = match compression {
        DsAvroCompression::Uncompressed => Codec::Null,
        DsAvroCompression::Deflate => Codec::Deflate,
        DsAvroCompression::Snappy => Codec::Snappy,
    };

    // Build Avro schema from DataFrame schema
    let schema = dataframe_to_avro_schema(df)?;

    // Create writer
    let file = File::create(path)?;
    let mut writer = Writer::with_codec(&schema, file, codec);

    // Write rows
    for row_idx in 0..df.height() {
        let mut record_fields = Vec::new();

        for (col_idx, column) in df.get_columns().iter().enumerate() {
            let field_name = df.get_column_names()[col_idx];
            let avro_value = polars_value_to_avro(column.get(row_idx)?)?;
            record_fields.push((field_name.to_string(), avro_value));
        }

        let record = AvroValue::Record(record_fields);
        writer.append(record)?;
    }

    writer.flush()?;
    Ok(())
}

/// Convert DataFrame schema to Avro schema
fn dataframe_to_avro_schema(df: &DataFrame) -> Result<Schema> {
    let mut fields = Vec::new();

    for (col_idx, column) in df.get_columns().iter().enumerate() {
        let field_name = df.get_column_names()[col_idx];
        let field_schema = polars_dtype_to_avro_schema(column.dtype())?;

        // Create field with name and schema
        fields.push(apache_avro::schema::RecordField {
            name: field_name.to_string(),
            doc: None,
            default: None,
            schema: field_schema,
            order: apache_avro::schema::RecordFieldOrder::Ascending,
            position: col_idx,
            custom_attributes: Default::default(),
        });
    }

    Ok(Schema::Record(apache_avro::schema::RecordSchema {
        name: apache_avro::schema::Name::new("record")?,
        aliases: None,
        doc: None,
        fields,
        lookup: Default::default(),
        attributes: Default::default(),
    }))
}

/// Convert Polars DataType to Avro Schema
fn polars_dtype_to_avro_schema(dtype: &DataType) -> Result<Schema> {
    match dtype {
        DataType::Boolean => Ok(Schema::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(Schema::Int),
        DataType::Int64 => Ok(Schema::Long),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok(Schema::Int),
        DataType::UInt64 => Ok(Schema::Long),
        DataType::Float32 => Ok(Schema::Float),
        DataType::Float64 => Ok(Schema::Double),
        DataType::String => Ok(Schema::String),
        DataType::Binary => Ok(Schema::Bytes),
        DataType::Null => Ok(Schema::Null),
        _ => Ok(Schema::String), // Default to string for unsupported types
    }
}

/// Convert Polars AnyValue to Avro Value
fn polars_value_to_avro(value: AnyValue) -> Result<AvroValue> {
    match value {
        AnyValue::Null => Ok(AvroValue::Null),
        AnyValue::Boolean(b) => Ok(AvroValue::Boolean(b)),
        AnyValue::Int8(i) => Ok(AvroValue::Int(i as i32)),
        AnyValue::Int16(i) => Ok(AvroValue::Int(i as i32)),
        AnyValue::Int32(i) => Ok(AvroValue::Int(i)),
        AnyValue::Int64(i) => Ok(AvroValue::Long(i)),
        AnyValue::UInt8(u) => Ok(AvroValue::Int(u as i32)),
        AnyValue::UInt16(u) => Ok(AvroValue::Int(u as i32)),
        AnyValue::UInt32(u) => Ok(AvroValue::Int(u as i32)),
        AnyValue::UInt64(u) => Ok(AvroValue::Long(u as i64)),
        AnyValue::Float32(f) => Ok(AvroValue::Float(f)),
        AnyValue::Float64(f) => Ok(AvroValue::Double(f)),
        AnyValue::String(s) => Ok(AvroValue::String(s.to_string())),
        AnyValue::Binary(b) => Ok(AvroValue::Bytes(b.to_vec())),
        _ => Ok(AvroValue::String(format!("{}", value))), // Default to string for unsupported types
    }
}
