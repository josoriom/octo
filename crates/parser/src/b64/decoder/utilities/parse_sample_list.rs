use crate::{
    b64::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF},
        utilities::{
            children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
            common::get_attr_text,
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::{
        schema::TagId,
        structs::{ReferenceableParamGroupRef, Sample, SampleList},
    },
};

#[inline]
pub(crate) fn parse_sample_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<SampleList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup.all_ids(TagId::SampleList).first().copied();

    let sample_ids: &[u32] = list_id
        .map(|id| children_lookup.ids_for(id, TagId::Sample))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::Sample));

    if sample_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let samples = sample_ids
        .iter()
        .map(|&sample_id| {
            let rows = owner_rows.get(sample_id);

            param_buffer.clear();
            children_lookup.get_param_rows_into(&owner_rows, sample_id, policy, &mut param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(&param_buffer);

            Sample {
                id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
                name: get_attr_text(rows, ACC_ATTR_NAME).unwrap_or_default(),
                referenceable_param_group_ref: children_lookup
                    .ids_for(sample_id, TagId::ReferenceableParamGroupRef)
                    .first()
                    .and_then(|&ref_id| get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF))
                    .map(|r| ReferenceableParamGroupRef { r#ref: r }),
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) = list_id
        .map(|id| {
            param_buffer.clear();
            children_lookup.get_param_rows_into(&owner_rows, id, policy, &mut param_buffer);
            parse_cv_and_user_params(&param_buffer)
        })
        .unwrap_or_default();

    Some(SampleList {
        count: Some(samples.len() as u32),
        samples,
        cv_params,
        user_params,
    })
}
