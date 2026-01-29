use std::collections::HashMap;

use crate::{
    b64::utilities::{
        common::{
            ChildIndex, OwnerRows, ParseCtx, b000_attr_text, child_params_for_parent,
            ids_for_parent, rows_for_owner,
        },
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_COUNT, ACC_ATTR_ID, ACC_ATTR_LOCATION, ACC_ATTR_NAME},
        schema::TagId,
        structs::{
            Contact, FileContent, FileDescription, ReferenceableParamGroupRef, SourceFile,
            SourceFileList,
        },
    },
};

/// <fileDescription>
#[inline]
pub fn parse_file_description(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<FileDescription> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut file_desc_id: Option<u32> = None;
    let mut fallback_from_file_content: Option<u32> = None;
    let mut fallback_from_source_file_list: Option<u32> = None;
    let mut fallback_from_contact: Option<u32> = None;
    let mut fallback_sfl_from_source_file: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::FileDescription => {
                if file_desc_id.is_none() {
                    file_desc_id = Some(m.owner_id);
                }
            }
            TagId::FileContent => {
                if fallback_from_file_content.is_none() && m.parent_index != 0 {
                    fallback_from_file_content = Some(m.parent_index);
                }
            }
            TagId::SourceFileList => {
                if fallback_from_source_file_list.is_none() && m.parent_index != 0 {
                    fallback_from_source_file_list = Some(m.parent_index);
                }
            }
            TagId::Contact => {
                if fallback_from_contact.is_none() && m.parent_index != 0 {
                    fallback_from_contact = Some(m.parent_index);
                }
            }
            TagId::SourceFile => {
                if fallback_sfl_from_source_file.is_none() && m.parent_index != 0 {
                    fallback_sfl_from_source_file = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let file_desc_id = file_desc_id
        .or(fallback_from_file_content)
        .or(fallback_from_source_file_list)
        .or(fallback_from_contact)
        .or_else(|| {
            // SourceFile -> SourceFileList(parent) -> FileDescription(parent of SFL row)
            let sfl_id = fallback_sfl_from_source_file?;
            owner_rows
                .get(&sfl_id)
                .and_then(|rows| rows.first())
                .map(|m| m.parent_index)
        })?;

    let ctx = ParseCtx {
        metadata,
        child_index,
        owner_rows: &owner_rows,
    };

    let file_content = parse_file_content(&ctx, file_desc_id);
    let source_file_list = parse_source_file_list(&ctx, file_desc_id);
    let contacts = parse_contacts(&ctx, file_desc_id);

    Some(FileDescription {
        file_content,
        source_file_list,
        contacts,
    })
}

/// <fileContent>
#[inline]
fn parse_file_content(ctx: &ParseCtx<'_>, file_desc_id: u32) -> FileContent {
    let ids = ids_for_parent(ctx, file_desc_id, TagId::FileContent);
    let file_content_id = ids.first().copied().unwrap_or(0);
    if file_content_id == 0 {
        return FileContent::default();
    }

    let rows = rows_for_owner(ctx.owner_rows, file_content_id);
    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, file_content_id);

    let (cv_params, user_params) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    FileContent {
        referenceable_param_group_refs: Vec::<ReferenceableParamGroupRef>::new(),
        cv_params,
        user_params,
    }
}

/// <sourceFileList>
#[inline]
fn parse_source_file_list(ctx: &ParseCtx<'_>, file_desc_id: u32) -> SourceFileList {
    let sfl_ids = ids_for_parent(ctx, file_desc_id, TagId::SourceFileList);
    let sfl_id = sfl_ids.first().copied().unwrap_or(0);
    if sfl_id == 0 {
        return SourceFileList {
            count: Some(0),
            source_file: Vec::new(),
        };
    }

    let sfl_rows = rows_for_owner(ctx.owner_rows, sfl_id);
    let count_attr = b000_attr_text(sfl_rows, ACC_ATTR_COUNT).and_then(|s| s.parse::<usize>().ok());

    let source_file_ids = ids_for_parent(ctx, sfl_id, TagId::SourceFile);
    if source_file_ids.is_empty() {
        return SourceFileList {
            count: count_attr.or(Some(0)),
            source_file: Vec::new(),
        };
    }

    let mut source_file = Vec::with_capacity(source_file_ids.len());
    for id in source_file_ids {
        source_file.push(parse_source_file(ctx, id));
    }

    SourceFileList {
        count: count_attr.or(Some(source_file.len())),
        source_file,
    }
}

/// <sourceFile>
#[inline]
fn parse_source_file(ctx: &ParseCtx<'_>, source_file_id: u32) -> SourceFile {
    let rows = rows_for_owner(ctx.owner_rows, source_file_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let name = b000_attr_text(rows, ACC_ATTR_NAME).unwrap_or_default();
    let location = b000_attr_text(rows, ACC_ATTR_LOCATION).unwrap_or_default();

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, source_file_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    SourceFile {
        id,
        name,
        location,
        referenceable_param_group_ref: Vec::<ReferenceableParamGroupRef>::new(),
        cv_param,
        user_param,
    }
}

/// <contact>
#[inline]
fn parse_contacts(ctx: &ParseCtx<'_>, file_desc_id: u32) -> Vec<Contact> {
    let contact_ids = ids_for_parent(ctx, file_desc_id, TagId::Contact);
    if contact_ids.is_empty() {
        return Vec::new();
    }

    let mut contacts = Vec::with_capacity(contact_ids.len());
    for id in contact_ids {
        contacts.push(parse_contact(ctx, id));
    }
    contacts
}

/// <contact>
#[inline]
fn parse_contact(ctx: &ParseCtx<'_>, contact_id: u32) -> Contact {
    let rows = rows_for_owner(ctx.owner_rows, contact_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, contact_id);

    let (cv_params, user_params) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    Contact {
        referenceable_param_group_refs: Vec::<ReferenceableParamGroupRef>::new(),
        cv_params,
        user_params,
    }
}
