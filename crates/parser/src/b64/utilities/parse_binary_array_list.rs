use crate::{
    BinaryDataArray, BinaryDataArrayList, CvParam, NumericType, UserParam,
    b64::utilities::common::{is_cv_prefix, unit_cv_ref, value_to_opt_string},
    decode::{Metadatum, MetadatumValue},
    mzml::{
        attr_meta::{
            ACC_ATTR_ARRAY_LENGTH, ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF,
            ACC_ATTR_DEFAULT_ARRAY_LENGTH, ACC_ATTR_ENCODED_LENGTH, CV_REF_ATTR,
        },
        cv_table,
        schema::TagId,
    },
};

/// <binaryDataArrayList>
#[inline]
pub fn parse_binary_data_array_list(metadata: &[&Metadatum]) -> Option<BinaryDataArrayList> {
    let list_id = metadata
        .iter()
        .find(|m| m.tag_id == TagId::BinaryDataArrayList)
        .map(|m| m.id)
        .or_else(|| {
            metadata
                .iter()
                .find(|m| m.tag_id == TagId::BinaryDataArray)
                .map(|m| m.parent_index)
        })?;

    let mut count: Option<usize> = None;
    let mut parent_default_len: Option<usize> = None;

    let mut tmp: Vec<(u32, BinaryDataArray)> = Vec::new();

    for m in metadata {
        if parent_default_len.is_none()
            && m.accession.as_deref().and_then(b000_tail) == Some(ACC_ATTR_DEFAULT_ARRAY_LENGTH)
        {
            parent_default_len = as_u32(&m.value).map(|v| v as usize);
        }

        if count.is_none()
            && m.tag_id == TagId::BinaryDataArrayList
            && m.id == list_id
            && m.accession.as_deref().and_then(b000_tail) == Some(ACC_ATTR_COUNT)
        {
            count = as_u32(&m.value).map(|v| v as usize);
            continue;
        }

        if m.tag_id != TagId::BinaryDataArray || m.parent_index != list_id {
            continue;
        }

        let id = m.id;
        let at = match tmp.iter().position(|(x, _)| *x == id) {
            Some(i) => i,
            None => {
                tmp.push((id, new_binary_data_array()));
                tmp.len() - 1
            }
        };

        apply_binary_data_array_metadatum(&mut tmp[at].1, m);
    }

    if tmp.is_empty() {
        return Some(BinaryDataArrayList {
            count: count.or(Some(0)),
            binary_data_arrays: Vec::new(),
        });
    }

    tmp.sort_unstable_by_key(|(id, _)| *id);

    let mut binary_data_arrays: Vec<BinaryDataArray> =
        tmp.into_iter().map(|(_, bda)| bda).collect();

    if let Some(len) = parent_default_len {
        for bda in &mut binary_data_arrays {
            if bda.array_length.is_none() {
                bda.array_length = Some(len);
            }
        }
    }

    Some(BinaryDataArrayList {
        count: count.or(Some(binary_data_arrays.len())),
        binary_data_arrays,
    })
}

/// <binaryDataArray>
#[inline]
fn new_binary_data_array() -> BinaryDataArray {
    BinaryDataArray {
        array_length: None,
        encoded_length: None,
        data_processing_ref: None,
        referenceable_param_group_refs: Vec::new(),
        cv_params: Vec::with_capacity(8),
        user_params: Vec::with_capacity(2),
        numeric_type: None,
        binary: None,
    }
}

#[inline]
fn apply_binary_data_array_metadatum(out: &mut BinaryDataArray, m: &Metadatum) {
    let Some(acc) = m.accession.as_deref() else {
        return;
    };
    let Some((prefix, tail)) = acc.split_once(':') else {
        return;
    };

    if prefix == CV_REF_ATTR {
        let Ok(tail_u32) = tail.parse::<u32>() else {
            return;
        };
        match tail_u32 {
            ACC_ATTR_ARRAY_LENGTH => out.array_length = as_u32(&m.value).map(|v| v as usize),
            ACC_ATTR_ENCODED_LENGTH => out.encoded_length = as_u32(&m.value).map(|v| v as usize),
            ACC_ATTR_DATA_PROCESSING_REF => out.data_processing_ref = as_string(&m.value),
            _ => {}
        }
        return;
    }

    let value = value_to_opt_string(&m.value);
    let unit_accession_str = m.unit_accession.as_deref();
    let unit_cv_ref = unit_cv_ref(unit_accession_str);

    if is_cv_prefix(prefix) {
        if prefix == "MS" {
            let new_ty = match tail {
                "1000519" => Some(NumericType::Int32),
                "1000521" => Some(NumericType::Float32),
                "1000522" => Some(NumericType::Int64),
                "1000523" => Some(NumericType::Float64),
                _ => None,
            };

            if let Some(nt) = new_ty {
                out.numeric_type = match out.numeric_type {
                    None => Some(nt),
                    Some(cur) if cur == nt => Some(cur),
                    Some(_) => None,
                };
            }
        }

        let unit_name = if let Some(ua) = unit_accession_str {
            cv_table::get(ua)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        } else {
            None
        };

        let name = cv_table::get(acc)
            .and_then(|v| v.as_str())
            .unwrap_or(acc)
            .to_owned();

        let unit_accession = m.unit_accession.clone();
        out.cv_params.push(CvParam {
            cv_ref: Some(prefix.to_owned()),
            accession: Some(acc.to_owned()),
            name,
            value,
            unit_cv_ref,
            unit_name,
            unit_accession,
        });
    } else {
        let unit_accession = m.unit_accession.clone();
        out.user_params.push(UserParam {
            name: acc.to_string(),
            r#type: None,
            unit_accession,
            unit_cv_ref,
            unit_name: None,
            value,
        });
    }
}

#[inline]
fn b000_tail(acc: &str) -> Option<u32> {
    let tail = acc.strip_prefix("B000:")?;
    tail.parse::<u32>().ok()
}

#[inline]
fn as_u32(v: &MetadatumValue) -> Option<u32> {
    match v {
        MetadatumValue::Number(f) => {
            if f.is_finite() && f.fract() == 0.0 && *f >= 0.0 && *f <= (u32::MAX as f64) {
                Some(*f as u32)
            } else {
                None
            }
        }
        MetadatumValue::Text(s) => s.parse::<u32>().ok(),
        MetadatumValue::Empty => None,
    }
}

#[inline]
fn as_string(v: &MetadatumValue) -> Option<String> {
    match v {
        MetadatumValue::Text(s) => Some(s.clone()),
        MetadatumValue::Number(f) => Some(f.to_string()),
        MetadatumValue::Empty => None,
    }
}
