use std::collections::{HashMap, HashSet};

use crate::{
    ScanList, ScanWindow, ScanWindowList,
    b64::utilities::{
        common::{
            ChildIndex, OwnerRows, ParseCtx, child_params_for_parent, ordered_unique_owner_ids,
            rows_for_owner, unique_ids,
        },
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{schema::TagId, structs::Scan},
};

/// <scanList>
#[inline]
pub fn parse_scan_list(metadata: &[&Metadatum], child_index: &ChildIndex) -> Option<ScanList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());
    let mut scan_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);
        if scan_list_id.is_none() && m.tag_id == TagId::ScanList {
            scan_list_id = Some(m.owner_id);
        }
    }

    let scan_list_id = scan_list_id?;

    let ctx = ParseCtx {
        metadata,
        child_index,
        owner_rows: &owner_rows,
    };

    let scan_list_rows = rows_for_owner(ctx.owner_rows, scan_list_id);

    let mut scan_list_params_meta: Vec<&Metadatum> = Vec::with_capacity(scan_list_rows.len() + 8);
    scan_list_params_meta.extend_from_slice(scan_list_rows);
    scan_list_params_meta.extend(child_params_for_parent(
        ctx.owner_rows,
        ctx.child_index,
        scan_list_id,
    ));

    let (cv_params, user_params) = parse_cv_and_user_params(&scan_list_params_meta);

    let mut scan_ids: Vec<u32> = unique_ids(ctx.child_index.ids(scan_list_id, TagId::Scan));
    if scan_ids.is_empty() {
        scan_ids = ordered_unique_owner_ids(ctx.metadata, TagId::Scan);
    }
    if scan_ids.is_empty() {
        return None;
    }

    let single_scan_id = if scan_ids.len() == 1 {
        Some(scan_ids[0])
    } else {
        None
    };
    let all_scan_window_ids = ordered_unique_owner_ids(ctx.metadata, TagId::ScanWindow);

    let mut scans = Vec::with_capacity(scan_ids.len());
    for scan_id in scan_ids {
        let scan_rows = rows_for_owner(ctx.owner_rows, scan_id);
        let scan_parent = scan_rows.first().map(|m| m.parent_index).unwrap_or(0);

        let scan_window_ids = scan_window_ids_for_scan(
            ctx.child_index,
            scan_id,
            scan_parent,
            single_scan_id,
            &all_scan_window_ids,
        );

        let scan_window_list = parse_scan_window_list(ctx.owner_rows, &scan_window_ids);

        scans.push(parse_scan(scan_rows, scan_window_list));
    }

    Some(ScanList {
        count: Some(scans.len()),
        cv_params,
        user_params,
        scans,
    })
}

/// <scan>
#[inline]
fn parse_scan(scan_rows: &[&Metadatum], scan_window_list: Option<ScanWindowList>) -> Scan {
    let (cv_params, user_params) = parse_cv_and_user_params(scan_rows);

    Scan {
        instrument_configuration_ref: None,
        external_spectrum_id: None,
        source_file_ref: None,
        spectrum_ref: None,
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        scan_window_list,
    }
}

/// <scanWindowList>
#[inline]
fn parse_scan_window_list(
    owner_rows: &HashMap<u32, Vec<&Metadatum>>,
    scan_window_ids: &[u32],
) -> Option<ScanWindowList> {
    if scan_window_ids.is_empty() {
        return None;
    }

    let mut scan_windows = Vec::with_capacity(scan_window_ids.len());
    for &sw_id in scan_window_ids {
        let sw_rows = rows_for_owner(owner_rows, sw_id);
        scan_windows.push(parse_scan_window(sw_rows));
    }

    Some(ScanWindowList {
        count: Some(scan_windows.len()),
        scan_windows,
    })
}

/// <scanWindow>
#[inline]
fn parse_scan_window(scan_window_rows: &[&Metadatum]) -> ScanWindow {
    let (cv_params, user_params) = parse_cv_and_user_params(scan_window_rows);

    ScanWindow {
        cv_params,
        user_params,
        ..Default::default()
    }
}

#[inline]
fn scan_window_ids_for_scan(
    child_index: &ChildIndex,
    scan_id: u32,
    scan_parent: u32,
    single_scan_id: Option<u32>,
    all_scan_window_ids: &[u32],
) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::new();

    out.extend_from_slice(child_index.ids(scan_id, TagId::ScanWindow));
    if out.is_empty() {
        out.extend_from_slice(child_index.ids(scan_parent, TagId::ScanWindow));
    }

    if out.is_empty() {
        for parent in [scan_id, scan_parent] {
            for &swl_id in child_index.ids(parent, TagId::ScanWindowList) {
                out.extend_from_slice(child_index.ids(swl_id, TagId::ScanWindow));
            }
        }
    }

    if out.is_empty() && single_scan_id == Some(scan_id) {
        out.extend_from_slice(all_scan_window_ids);
    }

    let mut seen = HashSet::with_capacity(out.len());
    out.retain(|id| seen.insert(*id));
    out
}
