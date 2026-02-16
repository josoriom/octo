use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::{get_attr_text, get_attr_u32},
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID, ACC_ATTR_LOCATION, ACC_ATTR_NAME},
        schema::TagId,
        structs::{Contact, FileContent, FileDescription, SourceFile, SourceFileList},
    },
};

#[inline]
pub fn parse_file_description(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<FileDescription> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let root_id = children_lookup
        .all_ids(TagId::FileDescription)
        .first()
        .copied()
        .or_else(|| {
            children_lookup
                .all_ids(TagId::SourceFileList)
                .first()
                .and_then(|&id| owner_rows.get(id).first().map(|m| m.parent_id))
        })
        .unwrap_or(0);

    Some(FileDescription {
        file_content: parse_file_content(&owner_rows, children_lookup, root_id),
        source_file_list: parse_source_file_list(&owner_rows, children_lookup, root_id),
        contacts: parse_contacts(&owner_rows, children_lookup, root_id),
    })
}

#[inline]
fn parse_file_content(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    root_id: u32,
) -> FileContent {
    let content_id = children_lookup
        .first_id(root_id, TagId::FileContent)
        .unwrap_or(root_id);
    let (cv_params, user_params) =
        parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, content_id));

    FileContent {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    }
}

#[inline]
fn parse_source_file_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    root_id: u32,
) -> SourceFileList {
    let list_id = children_lookup.first_id(root_id, TagId::SourceFileList);
    let ids = children_lookup.ids_for(list_id.unwrap_or(0), TagId::SourceFile);

    let count = list_id
        .and_then(|id| get_attr_u32(owner_rows.get(id), ACC_ATTR_COUNT))
        .map(|v| v as usize);

    let source_file = ids
        .iter()
        .map(|&id| {
            let rows = owner_rows.get(id);
            let (cv_param, user_param) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));

            SourceFile {
                id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
                name: get_attr_text(rows, ACC_ATTR_NAME).unwrap_or_default(),
                location: get_attr_text(rows, ACC_ATTR_LOCATION).unwrap_or_default(),
                referenceable_param_group_ref: Vec::new(),
                cv_param,
                user_param,
            }
        })
        .collect::<Vec<_>>();

    SourceFileList {
        count: count.or(Some(source_file.len())),
        source_file,
    }
}

#[inline]
fn parse_contacts(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    root_id: u32,
) -> Vec<Contact> {
    children_lookup
        .ids_for(root_id, TagId::Contact)
        .iter()
        .map(|&id| {
            let (cv_params, user_params) =
                parse_cv_and_user_params(&children_lookup.get_param_rows(owner_rows, id));
            Contact {
                referenceable_param_group_refs: Vec::new(),
                cv_params,
                user_params,
            }
        })
        .collect()
}
