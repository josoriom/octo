use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::b000_attr_text,
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
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());
    let mut list_id: Option<u32> = None;
    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);
        if list_id.is_none() && m.tag_id == TagId::ReferenceableParamGroupList {
            list_id = Some(m.id);
        }
    }

    let group_ids = if let Some(list_id) = list_id {
        let ids = children_lookup.ids_for(metadata, list_id, TagId::ReferenceableParamGroup);
        if ids.is_empty() {
            ChildrenLookup::all_ids(metadata, TagId::ReferenceableParamGroup)
        } else {
            ids
        }
    } else {
        ChildrenLookup::all_ids(metadata, TagId::ReferenceableParamGroup)
    };

    if group_ids.is_empty() {
        return None;
    }

    let mut referenceable_param_groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        referenceable_param_groups.push(parse_referenceable_param_group(
            metadata,
            children_lookup,
            &owner_rows,
            group_id,
        ));
    }

    let count = list_id
        .and_then(|id| {
            b000_attr_text(
                ChildrenLookup::rows_for_owner(&owner_rows, id),
                ACC_ATTR_COUNT,
            )
        })
        .and_then(|s| s.parse::<usize>().ok())
        .or(Some(referenceable_param_groups.len()));

    Some(ReferenceableParamGroupList {
        count,
        referenceable_param_groups,
    })
}

#[inline]
fn parse_referenceable_param_group<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    group_id: u32,
) -> ReferenceableParamGroup {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, group_id);
    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let child_rows = children_lookup.param_rows(metadata, owner_rows, group_id);
    let (cv_params, user_params) = if child_rows.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_rows.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_rows);
        parse_cv_and_user_params(&params_meta)
    };
    ReferenceableParamGroup {
        id,
        cv_params,
        user_params,
    }
}
