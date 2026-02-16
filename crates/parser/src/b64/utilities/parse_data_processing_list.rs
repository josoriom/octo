use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SOFTWARE_REF},
        schema::TagId,
        structs::{
            DataProcessing, DataProcessingList, ProcessingMethod, ReferenceableParamGroupRef,
        },
    },
};

#[inline]
pub fn parse_data_processing_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<DataProcessingList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::DataProcessingList)
        .first()
        .copied();
    let ids = list_id
        .map(|id| children_lookup.ids_for(id, TagId::DataProcessing))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::DataProcessing).to_vec());

    if ids.is_empty() {
        return None;
    }

    let data_processing: Vec<DataProcessing> = ids
        .iter()
        .map(|&id| parse_data_processing(children_lookup, &owner_rows, id))
        .collect();

    Some(DataProcessingList {
        count: Some(data_processing.len()),
        data_processing,
    })
}

#[inline]
fn parse_data_processing(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    data_processing_id: u32,
) -> DataProcessing {
    let rows = owner_rows.get(data_processing_id);

    DataProcessing {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        software_ref: get_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty()),
        processing_method: children_lookup
            .ids_for(data_processing_id, TagId::ProcessingMethod)
            .iter()
            .map(|&id| parse_processing_method(children_lookup, owner_rows, id))
            .collect(),
    }
}

#[inline]
fn parse_processing_method(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    processing_method_id: u32,
) -> ProcessingMethod {
    let rows = owner_rows.get(processing_method_id);
    let (cv_param, user_param) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, processing_method_id));

    ProcessingMethod {
        order: get_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse().ok()),
        software_ref: get_attr_text(rows, ACC_ATTR_SOFTWARE_REF).filter(|s| !s.is_empty()),
        referenceable_param_group_ref: parse_referenceable_param_group_refs(
            children_lookup,
            owner_rows,
            processing_method_id,
        ),
        cv_param,
        user_param,
    }
}

#[inline]
fn parse_referenceable_param_group_refs(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    children_lookup
        .ids_for(parent_id, TagId::ReferenceableParamGroupRef)
        .iter()
        .filter_map(|&id| {
            get_attr_text(owner_rows.get(id), ACC_ATTR_REF)
                .filter(|s| !s.is_empty())
                .map(|r| ReferenceableParamGroupRef { r#ref: r })
        })
        .collect()
}
