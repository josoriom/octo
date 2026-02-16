use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID},
        schema::TagId,
        structs::{ReferenceableParamGroup, ReferenceableParamGroupList},
    },
};

#[inline]
pub fn parse_referenceable_param_group_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<ReferenceableParamGroupList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup
        .all_ids(TagId::ReferenceableParamGroupList)
        .first()
        .copied();
    let ids = list_id
        .map(|id| children_lookup.ids_for(id, TagId::ReferenceableParamGroup))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| {
            children_lookup
                .all_ids(TagId::ReferenceableParamGroup)
                .to_vec()
        });

    if ids.is_empty() {
        return None;
    }

    let referenceable_param_groups = ids
        .into_iter()
        .map(|id| {
            let rows = owner_rows.get(id);
            let (cv_params, user_params) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(&owner_rows, id));
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
