use crate::{
    BinaryDataArray, BinaryDataArrayList, NumericType,
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::{get_attr_text, get_attr_u32},
        parse_cv_and_user_params,
    },
    mzml::{
        attr_meta::{
            ACC_ATTR_ARRAY_LENGTH, ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF,
            ACC_ATTR_ENCODED_LENGTH,
        },
        schema::TagId,
    },
};

#[inline]
pub fn parse_binary_data_array_list(
    rows_by_id: &OwnerRows,
    children_lookup: &ChildrenLookup,
    parent_entity_id: u32,
) -> Option<BinaryDataArrayList> {
    let list_id = children_lookup.first_id(parent_entity_id, TagId::BinaryDataArrayList);
    let array_ids =
        children_lookup.ids_for(list_id.unwrap_or(parent_entity_id), TagId::BinaryDataArray);

    if array_ids.is_empty() {
        return None;
    }

    let binary_data_arrays = array_ids
        .iter()
        .map(|&id| parse_binary_data_array(rows_by_id, children_lookup, id))
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
fn parse_binary_data_array(
    rows_by_id: &OwnerRows,
    children_lookup: &ChildrenLookup,
    array_id: u32,
) -> BinaryDataArray {
    let rows = rows_by_id.get(array_id);
    let parameter_rows = children_lookup.get_param_rows(rows_by_id, array_id);
    let (cv_params, user_params) = parse_cv_and_user_params(&parameter_rows);

    let numeric_type = cv_params.iter().find_map(|p| match p.accession.as_deref() {
        Some("MS:1000519") => Some(NumericType::Int32),
        Some("MS:1000521") => Some(NumericType::Float32),
        Some("MS:1000522") => Some(NumericType::Int64),
        Some("MS:1000523") => Some(NumericType::Float64),
        _ => None,
    });

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
