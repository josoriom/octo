use crate::{
    b64::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID},
        utilities::{
            children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
            common::get_attr_text,
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::{
        schema::TagId,
        structs::{ReferenceableParamGroup, ReferenceableParamGroupList},
    },
};

#[inline]
pub(crate) fn parse_referenceable_param_group_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<ReferenceableParamGroupList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup
        .all_ids(TagId::ReferenceableParamGroupList)
        .first()
        .copied();

    let group_ids: &[u32] = list_id
        .map(|id| children_lookup.ids_for(id, TagId::ReferenceableParamGroup))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::ReferenceableParamGroup));

    if group_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let referenceable_param_groups = group_ids
        .iter()
        .map(|&group_id| {
            let rows = owner_rows.get(group_id);

            param_buffer.clear();
            children_lookup.get_param_rows_into(&owner_rows, group_id, policy, &mut param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(&param_buffer);

            ReferenceableParamGroup {
                id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
                cv_params,
                user_params,
            }
        })
        .collect::<Vec<_>>();

    let count = list_id
        .and_then(|id| get_attr_text(owner_rows.get(id), ACC_ATTR_COUNT))
        .and_then(|s| s.parse::<usize>().ok());

    Some(ReferenceableParamGroupList {
        count: count.or(Some(referenceable_param_groups.len())),
        referenceable_param_groups,
    })
}
