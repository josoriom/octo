use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
        common::{get_attr_text, get_attr_u32},
        parse_cv_and_user_params,
    },
    decoder::decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID, ACC_ATTR_LOCATION, ACC_ATTR_NAME},
        schema::TagId,
        structs::{Contact, FileContent, FileDescription, SourceFile, SourceFileList},
    },
};

#[inline]
pub fn parse_file_description<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<FileDescription> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
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

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    Some(FileDescription {
        file_content: parse_file_content(
            &owner_rows,
            children_lookup,
            root_id,
            policy,
            &mut param_buffer,
        ),
        source_file_list: parse_source_file_list(
            &owner_rows,
            children_lookup,
            root_id,
            policy,
            &mut param_buffer,
        ),
        contacts: parse_contacts(
            &owner_rows,
            children_lookup,
            root_id,
            policy,
            &mut param_buffer,
        ),
    })
}

#[inline]
fn parse_file_content<'a, P: MetadataPolicy>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    root_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> FileContent {
    let content_id = children_lookup
        .ids_for(root_id, TagId::FileContent)
        .first()
        .copied()
        .unwrap_or(root_id);

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, content_id, policy, param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

    FileContent {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
    }
}

#[inline]
fn parse_source_file_list<'a, P: MetadataPolicy>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    root_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> SourceFileList {
    let source_file_list_id = children_lookup
        .ids_for(root_id, TagId::SourceFileList)
        .first()
        .copied();

    let source_file_ids: &[u32] = match source_file_list_id {
        Some(id) => children_lookup.ids_for(id, TagId::SourceFile),
        None => &[],
    };

    let count = source_file_list_id
        .and_then(|id| get_attr_u32(owner_rows.get(id), ACC_ATTR_COUNT))
        .map(|v| v as usize);

    let source_file = source_file_ids
        .iter()
        .map(|&source_file_id| {
            let rows = owner_rows.get(source_file_id);

            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, source_file_id, policy, param_buffer);
            let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

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
fn parse_contacts<'a, P: MetadataPolicy>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    root_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Vec<Contact> {
    children_lookup
        .ids_for(root_id, TagId::Contact)
        .iter()
        .map(|&contact_id| {
            param_buffer.clear();
            children_lookup.get_param_rows_into(owner_rows, contact_id, policy, param_buffer);
            let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

            Contact {
                referenceable_param_group_refs: Vec::new(),
                cv_params,
                user_params,
            }
        })
        .collect()
}
