use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::b000_attr_text,
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

#[inline]
pub fn parse_scan_settings_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<ScanSettingsList> {
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);
        if list_id.is_none()
            && matches!(
                m.tag_id,
                TagId::ScanSettingsList | TagId::AcquisitionSettingsList
            )
        {
            list_id = Some(m.id);
        }
    }

    let tags = [TagId::ScanSettings, TagId::AcquisitionSettings];

    let ids = if let Some(list_id) = list_id {
        let direct = children_lookup.ids_for_tags(metadata, list_id, &tags);
        if direct.is_empty() {
            let mut out = Vec::new();
            let mut seen = std::collections::HashSet::with_capacity(metadata.len().min(1024));
            for m in metadata {
                if matches!(m.tag_id, TagId::ScanSettings | TagId::AcquisitionSettings)
                    && seen.insert(m.id)
                {
                    out.push(m.id);
                }
            }
            out
        } else {
            direct
        }
    } else {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::with_capacity(metadata.len().min(1024));
        for m in metadata {
            if matches!(m.tag_id, TagId::ScanSettings | TagId::AcquisitionSettings)
                && seen.insert(m.id)
            {
                out.push(m.id);
            }
        }
        out
    };

    if ids.is_empty() {
        return None;
    }

    let mut scan_settings = Vec::with_capacity(ids.len());
    for id in ids {
        scan_settings.push(parse_scan_settings(
            metadata,
            children_lookup,
            &owner_rows,
            id,
        ));
    }

    Some(ScanSettingsList {
        count: Some(scan_settings.len()),
        scan_settings,
    })
}

#[inline]
fn parse_scan_settings<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    scan_settings_id: u32,
) -> ScanSettings {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, scan_settings_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).filter(|s| !s.is_empty());
    let instrument_configuration_ref =
        b000_attr_text(rows, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF).filter(|s| !s.is_empty());

    let referenceable_param_group_refs = parse_referenceable_param_group_refs(
        metadata,
        children_lookup,
        owner_rows,
        scan_settings_id,
    );

    let child_rows = children_lookup.param_rows(metadata, owner_rows, scan_settings_id);

    let (cv_params, user_params) = if child_rows.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params = Vec::with_capacity(rows.len() + child_rows.len());
        params.extend_from_slice(rows);
        params.extend(child_rows);
        parse_cv_and_user_params(&params)
    };

    let source_file_ref_list =
        parse_source_file_ref_list(metadata, children_lookup, owner_rows, scan_settings_id);
    let target_list = parse_target_list(metadata, children_lookup, owner_rows, scan_settings_id);

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

#[inline]
fn parse_source_file_ref_list<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    scan_settings_id: u32,
) -> Option<SourceFileRefList> {
    let list_ids = children_lookup.ids_for(metadata, scan_settings_id, TagId::SourceFileRefList);

    if let Some(list_id) = list_ids.first().copied() {
        let ref_ids = children_lookup.ids_for(metadata, list_id, TagId::SourceFileRef);

        if !ref_ids.is_empty() {
            let mut source_file_refs = Vec::with_capacity(ref_ids.len());
            for id in ref_ids {
                let rows = ChildrenLookup::rows_for_owner(owner_rows, id);
                if let Some(r) = b000_attr_text(rows, ACC_ATTR_REF).filter(|s| !s.is_empty()) {
                    source_file_refs.push(SourceFileRef { r#ref: r });
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

    let sf_list_ids = children_lookup.ids_for(metadata, scan_settings_id, TagId::SourceFileList);
    let list_id = sf_list_ids.first().copied()?;

    let sf_ids = children_lookup.ids_for(metadata, list_id, TagId::SourceFile);
    if sf_ids.is_empty() {
        return None;
    }

    let mut source_file_refs = Vec::with_capacity(sf_ids.len());
    for id in sf_ids {
        let rows = ChildrenLookup::rows_for_owner(owner_rows, id);
        if let Some(sf_id) = b000_attr_text(rows, ACC_ATTR_ID).filter(|s| !s.is_empty()) {
            source_file_refs.push(SourceFileRef { r#ref: sf_id });
        }
    }

    (!source_file_refs.is_empty()).then(|| SourceFileRefList {
        count: Some(source_file_refs.len()),
        source_file_refs,
    })
}

#[inline]
fn parse_target_list<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    scan_settings_id: u32,
) -> Option<TargetList> {
    let list_ids = children_lookup.ids_for(metadata, scan_settings_id, TagId::TargetList);

    let mut target_ids = if let Some(list_id) = list_ids.first().copied() {
        children_lookup.ids_for(metadata, list_id, TagId::Target)
    } else {
        Vec::new()
    };

    if target_ids.is_empty() {
        target_ids = children_lookup.ids_for(metadata, scan_settings_id, TagId::Target);
    }

    if !target_ids.is_empty() {
        let mut targets = Vec::with_capacity(target_ids.len());
        for id in target_ids {
            targets.push(parse_target(metadata, children_lookup, owner_rows, id));
        }
        return Some(TargetList {
            count: Some(targets.len()),
            targets,
        });
    }

    let cv_ids = children_lookup.ids_for(metadata, scan_settings_id, TagId::CvParam);
    if cv_ids.is_empty() {
        return None;
    }

    let mut target_cv = Vec::new();
    for cv_id in cv_ids {
        let rows = ChildrenLookup::rows_for_owner(owner_rows, cv_id);
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

    let mut targets = Vec::new();
    let mut cur = Vec::new();

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

#[inline]
fn parse_target<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    target_id: u32,
) -> Target {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, target_id);

    let referenceable_param_group_refs =
        parse_referenceable_param_group_refs(metadata, children_lookup, owner_rows, target_id);

    let child_rows = children_lookup.param_rows(metadata, owner_rows, target_id);

    let (cv_params, user_params) = if child_rows.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params = Vec::with_capacity(rows.len() + child_rows.len());
        params.extend_from_slice(rows);
        params.extend(child_rows);
        parse_cv_and_user_params(&params)
    };

    Target {
        referenceable_param_group_refs,
        cv_params,
        user_params,
    }
}

#[inline]
fn parse_referenceable_param_group_refs<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    let ref_ids = children_lookup.ids_for(metadata, parent_id, TagId::ReferenceableParamGroupRef);
    if ref_ids.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(ref_ids.len());
    for ref_id in ref_ids {
        let ref_rows = ChildrenLookup::rows_for_owner(owner_rows, ref_id);
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF).filter(|s| !s.is_empty()) {
            out.push(ReferenceableParamGroupRef { r#ref: r });
        }
    }

    out
}
