use std::collections::HashMap;

use crate::{
    b64::utilities::{
        common::{
            ChildIndex, ParseCtx, b000_attr_text, child_params_for_parent, ids_for_parent,
            ids_for_parent_tags, rows_for_owner,
        },
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_REF},
        schema::TagId,
        structs::{
            ReferenceableParamGroupRef, ScanSettings, ScanSettingsList, SourceFileRef,
            SourceFileRefList, Target, TargetList,
        },
    },
};

/// <scanSettingsList> / <acquisitionSettingsList>
#[inline]
pub fn parse_scan_settings_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<ScanSettingsList> {
    let mut owner_rows: HashMap<u32, Vec<&Metadatum>> = HashMap::with_capacity(metadata.len());
    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::ScanSettingsList | TagId::AcquisitionSettingsList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::ScanSettings | TagId::AcquisitionSettings => {
                if fallback_list_id.is_none() {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let list_id = list_id.or(fallback_list_id)?;

    let ctx = ParseCtx {
        metadata,
        child_index,
        owner_rows: &owner_rows,
    };

    let scan_settings_ids = ids_for_parent_tags(
        &ctx,
        list_id,
        &[TagId::ScanSettings, TagId::AcquisitionSettings],
    );

    if scan_settings_ids.is_empty() {
        return None;
    }

    let mut scan_settings = Vec::with_capacity(scan_settings_ids.len());
    for id in scan_settings_ids {
        scan_settings.push(parse_scan_settings(&ctx, id));
    }

    Some(ScanSettingsList {
        count: Some(scan_settings.len()),
        scan_settings,
    })
}

/// <scanSettings> / <acquisitionSettings>
#[inline]
fn parse_scan_settings(ctx: &ParseCtx<'_>, scan_settings_id: u32) -> ScanSettings {
    let rows = rows_for_owner(ctx.owner_rows, scan_settings_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).filter(|s| !s.is_empty());
    let instrument_configuration_ref =
        b000_attr_text(rows, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF).filter(|s| !s.is_empty());

    let referenceable_param_group_refs =
        parse_referenceable_param_group_refs(ctx, scan_settings_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, scan_settings_id);

    let (cv_params, user_params) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    let source_file_ref_list = parse_source_file_ref_list(ctx, scan_settings_id);
    let target_list = parse_target_list(ctx, scan_settings_id);

    ScanSettings {
        id,
        instrument_configuration_ref,
        referenceable_param_group_refs,
        cv_params,
        user_params,
        source_file_ref_list,
        target_list,
    }
}

/// <sourceFileRefList> OR (mzML 0.99.x) <sourceFileList>
#[inline]
fn parse_source_file_ref_list(
    ctx: &ParseCtx<'_>,
    scan_settings_id: u32,
) -> Option<SourceFileRefList> {
    let list_ids = ids_for_parent(ctx, scan_settings_id, TagId::SourceFileRefList);

    if let Some(list_id) = list_ids.first().copied() {
        let ref_ids = ids_for_parent(ctx, list_id, TagId::SourceFileRef);

        if !ref_ids.is_empty() {
            let mut source_file_refs = Vec::with_capacity(ref_ids.len());
            for id in ref_ids {
                let rows = rows_for_owner(ctx.owner_rows, id);
                if let Some(r) = b000_attr_text(rows, ACC_ATTR_REF) {
                    if !r.is_empty() {
                        source_file_refs.push(SourceFileRef { r#ref: r });
                    }
                }
            }

            if !source_file_refs.is_empty() {
                return Some(SourceFileRefList {
                    count: Some(source_file_refs.len()),
                    source_file_refs,
                });
            }
        }
    }

    let sf_list_ids = ids_for_parent(ctx, scan_settings_id, TagId::SourceFileList);
    let list_id = sf_list_ids.first().copied()?;

    let sf_ids = ids_for_parent(ctx, list_id, TagId::SourceFile);
    if sf_ids.is_empty() {
        return None;
    }

    let mut source_file_refs = Vec::with_capacity(sf_ids.len());
    for id in sf_ids {
        let rows = rows_for_owner(ctx.owner_rows, id);
        if let Some(sf_id) = b000_attr_text(rows, ACC_ATTR_ID) {
            if !sf_id.is_empty() {
                source_file_refs.push(SourceFileRef { r#ref: sf_id });
            }
        }
    }

    (!source_file_refs.is_empty()).then(|| SourceFileRefList {
        count: Some(source_file_refs.len()),
        source_file_refs,
    })
}

/// <targetList>
#[inline]
fn parse_target_list(ctx: &ParseCtx<'_>, scan_settings_id: u32) -> Option<TargetList> {
    let list_ids = ids_for_parent(ctx, scan_settings_id, TagId::TargetList);

    let mut target_ids: Vec<u32> = Vec::new();

    if let Some(list_id) = list_ids.first().copied() {
        target_ids = ids_for_parent(ctx, list_id, TagId::Target);
    }

    if target_ids.is_empty() {
        target_ids = ids_for_parent(ctx, scan_settings_id, TagId::Target);
    }

    if !target_ids.is_empty() {
        let mut targets = Vec::with_capacity(target_ids.len());
        for id in target_ids {
            targets.push(parse_target(ctx, id));
        }
        return Some(TargetList {
            count: Some(targets.len()),
            targets,
        });
    }

    let cv_ids = ids_for_parent(ctx, scan_settings_id, TagId::CvParam);
    if cv_ids.is_empty() {
        return None;
    }

    let mut target_cv: Vec<crate::mzml::structs::CvParam> = Vec::new();
    for cv_id in cv_ids {
        let rows = rows_for_owner(ctx.owner_rows, cv_id);
        let (mut cv_params, _user_params) = parse_cv_and_user_params(rows);
        target_cv.append(&mut cv_params);
    }

    target_cv.retain(|p| {
        p.accession
            .as_deref()
            .map(|a| {
                a.ends_with("1000827")
                    || a.ends_with("1001225")
                    || a.ends_with("1000502")
                    || a.ends_with("1000747")
            })
            .unwrap_or(false)
    });

    if target_cv.is_empty() {
        return None;
    }

    let mut targets: Vec<Target> = Vec::new();
    let mut cur: Vec<crate::mzml::structs::CvParam> = Vec::new();

    for p in target_cv {
        let is_boundary = p
            .accession
            .as_deref()
            .map(|a| a.ends_with("1000827"))
            .unwrap_or(false);

        if is_boundary && !cur.is_empty() {
            targets.push(Target {
                referenceable_param_group_refs: Vec::new(),
                cv_params: cur,
                user_params: Vec::new(),
            });
            cur = Vec::new();
        }

        cur.push(p);
    }

    if !cur.is_empty() {
        targets.push(Target {
            referenceable_param_group_refs: Vec::new(),
            cv_params: cur,
            user_params: Vec::new(),
        });
    }

    (!targets.is_empty()).then(|| TargetList {
        count: Some(targets.len()),
        targets,
    })
}

/// <target>
#[inline]
fn parse_target(ctx: &ParseCtx<'_>, target_id: u32) -> Target {
    let rows = rows_for_owner(ctx.owner_rows, target_id);

    let referenceable_param_group_refs = parse_referenceable_param_group_refs(ctx, target_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, target_id);

    let (cv_params, user_params) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    Target {
        referenceable_param_group_refs,
        cv_params,
        user_params,
    }
}

/// <referenceableParamGroupRef>
#[inline]
fn parse_referenceable_param_group_refs(
    ctx: &ParseCtx<'_>,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    let ref_ids = ids_for_parent(ctx, parent_id, TagId::ReferenceableParamGroupRef);
    if ref_ids.is_empty() {
        return Vec::new();
    }

    let mut out: Vec<ReferenceableParamGroupRef> = Vec::with_capacity(ref_ids.len());
    for ref_id in ref_ids {
        let ref_rows = rows_for_owner(ctx.owner_rows, ref_id);
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                out.push(ReferenceableParamGroupRef { r#ref: r });
            }
        }
    }

    out
}
