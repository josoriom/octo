use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use miniz_oxide::inflate::decompress_to_vec_zlib;
use quick_xml::events::BytesStart;
use std::io::BufRead;

use crate::{
    BinaryData, BinaryDataArray, BinaryDataArrayList, NumericType,
    mzml::{
        schema::TagId,
        utilities::{
            ParamCollector, ParseError, ParsingWorkspace, attr, attr_usize, read_base64_binary,
            read_cv_param, read_ref_group_ref, read_user_param,
        },
    },
};

pub(crate) fn parse_bda_list<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<BinaryDataArrayList, ParseError> {
    let mut list = BinaryDataArrayList {
        count: attr_usize(start, b"count"),
        ..Default::default()
    };
    ws.for_each_child(start, |ws, event| {
        let (tag, element, is_open) = event.into_parts();
        if tag != TagId::BinaryDataArray {
            return Ok(false);
        }
        if is_open {
            list.binary_data_arrays.push(parse_bda(ws, &element)?);
        } else {
            list.binary_data_arrays.push(BinaryDataArray {
                array_length: attr_usize(&element, b"arrayLength"),
                encoded_length: attr_usize(&element, b"encodedLength"),
                data_processing_ref: attr(&element, b"dataProcessingRef"),
                ..Default::default()
            });
        }
        Ok(true)
    })?;
    Ok(list)
}

pub(crate) fn parse_bda<R: BufRead>(
    ws: &mut ParsingWorkspace<R>,
    start: &BytesStart<'_>,
) -> Result<BinaryDataArray, ParseError> {
    let mut bda = BinaryDataArray {
        array_length: attr_usize(start, b"arrayLength"),
        encoded_length: attr_usize(start, b"encodedLength"),
        data_processing_ref: attr(start, b"dataProcessingRef"),
        ..Default::default()
    };
    let mut raw_b64: Vec<u8> = Vec::new();

    ws.for_each_child(start, |ws, event| {
        let (tag, element, _) = event.into_parts();
        match tag {
            TagId::CvParam => {
                bda.receive_cv(read_cv_param(&element));
                Ok(true)
            }
            TagId::UserParam => {
                bda.receive_user(read_user_param(&element));
                Ok(true)
            }
            TagId::ReferenceableParamGroupRef => {
                bda.receive_ref_group(read_ref_group_ref(&element));
                Ok(true)
            }
            TagId::Binary => {
                if let Some(len) = bda.encoded_length {
                    raw_b64.reserve(len);
                }
                let closing = element.name().as_ref().to_vec();
                read_base64_binary(ws, &closing, &mut raw_b64)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    })?;

    let encoding = encoding_for_array(&bda);
    bda.numeric_type = Some(encoding.numeric_type);

    if !raw_b64.is_empty() {
        let mut decoded = Vec::with_capacity(raw_b64.len() * 3 / 4 + 8);
        STANDARD.decode_vec(&raw_b64, &mut decoded)?;
        if encoding.is_zlib_compressed {
            decoded = decompress_to_vec_zlib(&decoded)
                .map_err(|e| ParseError::Decompress(format!("{e:?}")))?;
        }
        bda.binary = Some(decode_binary_data(
            encoding.numeric_type,
            &decoded,
            bda.array_length,
        ));
    }

    Ok(bda)
}

#[derive(Debug, Clone, Copy)]
struct BinaryArrayEncoding {
    is_zlib_compressed: bool,
    numeric_type: NumericType,
}

fn encoding_for_array(bda: &BinaryDataArray) -> BinaryArrayEncoding {
    let has = |acc: &str| {
        bda.cv_params
            .iter()
            .any(|p| p.accession.as_deref() == Some(acc))
    };
    let is_zlib_compressed = has("MS:1000574");
    let (f64, f32, f16) = (has("MS:1000523"), has("MS:1000521"), has("MS:1000520"));
    let (i64, i32, i16) = (has("MS:1000522"), has("MS:1001479"), has("MS:1000519"));

    let numeric_type = if let Some(declared) = bda.numeric_type {
        declared
    } else if f16 && !f32 && !f64 {
        NumericType::Float16
    } else if i16 && !i32 && !i64 {
        NumericType::Int16
    } else if i32 && !i64 {
        NumericType::Int32
    } else if i64 && !f64 && !f32 {
        NumericType::Int64
    } else if f64 && !f32 && !i64 {
        NumericType::Float64
    } else if f32 && !f64 && !i64 {
        NumericType::Float32
    } else if i64 {
        NumericType::Int64
    } else if f64 {
        NumericType::Float64
    } else if f32 {
        NumericType::Float32
    } else {
        NumericType::Float64
    };

    BinaryArrayEncoding {
        is_zlib_compressed,
        numeric_type,
    }
}

fn decode_binary_data(
    numeric_type: NumericType,
    decoded: &[u8],
    array_length: Option<usize>,
) -> BinaryData {
    match numeric_type {
        NumericType::Float64 => {
            BinaryData::F64(decode_packed_numeric_bytes(decoded, 8, array_length, |c| {
                f64::from_le_bytes(c.try_into().unwrap())
            }))
        }
        NumericType::Float32 => {
            BinaryData::F32(decode_packed_numeric_bytes(decoded, 4, array_length, |c| {
                f32::from_le_bytes(c.try_into().unwrap())
            }))
        }
        NumericType::Float16 => {
            BinaryData::F16(decode_packed_numeric_bytes(decoded, 2, array_length, |c| {
                u16::from_le_bytes(c.try_into().unwrap())
            }))
        }
        NumericType::Int64 => {
            BinaryData::I64(decode_packed_numeric_bytes(decoded, 8, array_length, |c| {
                i64::from_le_bytes(c.try_into().unwrap())
            }))
        }
        NumericType::Int32 => {
            BinaryData::I32(decode_packed_numeric_bytes(decoded, 4, array_length, |c| {
                i32::from_le_bytes(c.try_into().unwrap())
            }))
        }
        NumericType::Int16 => {
            BinaryData::I16(decode_packed_numeric_bytes(decoded, 2, array_length, |c| {
                i16::from_le_bytes(c.try_into().unwrap())
            }))
        }
    }
}

fn decode_packed_numeric_bytes<T, F>(
    bytes: &[u8],
    stride: usize,
    declared_length: Option<usize>,
    from_le: F,
) -> Vec<T>
where
    F: Fn(&[u8]) -> T,
{
    let available = (bytes.len() - bytes.len() % stride) / stride;
    let target = declared_length.unwrap_or(available).min(available);
    if target == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(target);
    for chunk in bytes[..target * stride].chunks_exact(stride) {
        out.push(from_le(chunk));
    }
    out
}
