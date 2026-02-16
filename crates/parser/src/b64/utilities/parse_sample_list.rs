use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_NAME, ACC_ATTR_REF},
        schema::TagId,
        structs::{ReferenceableParamGroupRef, Sample, SampleList},
    },
};

#[inline]
pub fn parse_sample_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<SampleList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup.all_ids(TagId::SampleList).first().copied();
    let ids = list_id
        .map(|id| children_lookup.ids_for(id, TagId::Sample))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::Sample).to_vec());

    if ids.is_empty() {
        return None;
    }

    let samples = ids
        .into_iter()
        .map(|id| {
            let rows = owner_rows.get(id);
            let (cv_params, user_params) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(&owner_rows, id));

            Sample {
                id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
                name: get_attr_text(rows, ACC_ATTR_NAME).unwrap_or_default(),
                referenceable_param_group_ref: children_lookup
                    .ids_for(id, TagId::ReferenceableParamGroupRef)
                    .first()
                    .and_then(|&ref_id| get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF))
                    .map(|r| ReferenceableParamGroupRef { r#ref: r }),
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    let (cv_params, user_params) = list_id
        .map(|id| parse_cv_and_user_params(&children_lookup.get_param_rows(&owner_rows, id)))
        .unwrap_or_default();

    Some(SampleList {
        count: Some(samples.len() as u32),
        samples,
        cv_params,
        user_params,
    })
}
