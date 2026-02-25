use crate::{
    BinaryDataArray, BinaryDataArrayList, CvParam, NumericType,
    b64::{
        attr_meta::{
            ACC_ATTR_ARRAY_LENGTH, ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF,
            ACC_ATTR_ENCODED_LENGTH,
        },
        utilities::{
            children_lookup::{ChildrenLookup, DefaultMetadataPolicy, MetadataPolicy, OwnerRows},
            common::{get_attr_text, get_attr_u32},
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::schema::TagId,
};

const ACC_NUMERIC_INT32: &str = "MS:1000519";
const ACC_NUMERIC_FLOAT32: &str = "MS:1000521";
const ACC_NUMERIC_INT64: &str = "MS:1000522";
const ACC_NUMERIC_FLOAT64: &str = "MS:1000523";

#[inline]
pub(crate) fn parse_binary_data_array_list(
    rows_by_id: &OwnerRows,
    children_lookup: &ChildrenLookup,
    parent_entity_id: u32,
) -> Option<BinaryDataArrayList> {
    let list_ids = children_lookup.ids_for(parent_entity_id, TagId::BinaryDataArrayList);
    let list_id = list_ids.first().copied();

    let array_ids: &[u32] = match list_id {
        Some(id) => children_lookup.ids_for(id, TagId::BinaryDataArray),
        None => children_lookup.ids_for(parent_entity_id, TagId::BinaryDataArray),
    };

    if array_ids.is_empty() {
        return None;
    }

    let policy = DefaultMetadataPolicy;
    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let binary_data_arrays = array_ids
        .iter()
        .map(|&id| {
            parse_binary_data_array(rows_by_id, children_lookup, id, &policy, &mut param_buffer)
        })
        .collect::<Vec<_>>();

    let list_count = list_id
        .and_then(|id| get_attr_u32(rows_by_id.get(id), ACC_ATTR_COUNT))
        .map(|v| v as usize);

    Some(BinaryDataArrayList {
        count: list_count.or(Some(binary_data_arrays.len())),
        binary_data_arrays,
    })
}

#[inline]
fn parse_binary_data_array<'a, P: MetadataPolicy>(
    rows_by_id: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    array_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> BinaryDataArray {
    param_buffer.clear();
    children_lookup.get_param_rows_into(rows_by_id, array_id, policy, param_buffer);

    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);
    let numeric_type = numeric_type_from_cv_params(&cv_params);
    let rows = rows_by_id.get(array_id);

    BinaryDataArray {
        array_length: get_attr_u32(rows, ACC_ATTR_ARRAY_LENGTH).map(|v| v as usize),
        encoded_length: get_attr_u32(rows, ACC_ATTR_ENCODED_LENGTH).map(|v| v as usize),
        data_processing_ref: get_attr_text(rows, ACC_ATTR_DATA_PROCESSING_REF),
        numeric_type,
        cv_params,
        user_params,
        binary: None,
        referenceable_param_group_refs: Vec::new(),
    }
}

#[inline]
fn numeric_type_from_cv_params(cv_params: &[CvParam]) -> Option<NumericType> {
    cv_params.iter().find_map(|p| match p.accession.as_deref() {
        Some(ACC_NUMERIC_INT32) => Some(NumericType::Int32),
        Some(ACC_NUMERIC_FLOAT32) => Some(NumericType::Float32),
        Some(ACC_NUMERIC_INT64) => Some(NumericType::Int64),
        Some(ACC_NUMERIC_FLOAT64) => Some(NumericType::Float64),
        _ => None,
    })
}
