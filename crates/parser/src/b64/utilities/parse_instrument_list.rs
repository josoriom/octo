use std::collections::HashMap;

use crate::{
    b64::utilities::{
        common::{
            ChildIndex, OwnerRows, ParseCtx, b000_attr_text, child_params_for_parent,
            ids_for_parent, rows_for_owner, unique_ids,
        },
        parse_cv_and_user_params,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SCAN_SETTINGS_REF},
        schema::TagId,
        structs::{
            Analyzer, ComponentList, Detector, Instrument, InstrumentList, InstrumentSoftwareRef,
            ReferenceableParamGroupRef, ScanSettingsRef, Source,
        },
    },
};

/// <instrumentConfigurationList>
#[inline]
pub fn parse_instrument_list(
    metadata: &[&Metadatum],
    child_index: &ChildIndex,
) -> Option<InstrumentList> {
    let mut owner_rows: OwnerRows<'_> = HashMap::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.owner_id).or_default().push(m);

        match m.tag_id {
            TagId::InstrumentConfigurationList => {
                if list_id.is_none() {
                    list_id = Some(m.owner_id);
                }
            }
            TagId::Instrument => {
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

    let instrument_ids = ids_for_parent(&ctx, list_id, TagId::Instrument);
    if instrument_ids.is_empty() {
        return None;
    }

    let mut instrument = Vec::with_capacity(instrument_ids.len());
    for id in instrument_ids {
        instrument.push(parse_instrument(&ctx, id));
    }

    Some(InstrumentList {
        count: Some(instrument.len()),
        instrument,
    })
}

/// <instrumentConfiguration>
#[inline]
fn parse_instrument(ctx: &ParseCtx<'_>, instrument_id: u32) -> Instrument {
    let rows = rows_for_owner(ctx.owner_rows, instrument_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();

    let scan_settings_ref = b000_attr_text(rows, ACC_ATTR_SCAN_SETTINGS_REF)
        .filter(|s| !s.is_empty())
        .map(|s| ScanSettingsRef { r#ref: s });

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(ctx.owner_rows, ctx.child_index, instrument_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, instrument_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    let component_list = parse_component_list(ctx, instrument_id);

    let software_ref = parse_instrument_software_ref(ctx, instrument_id);

    Instrument {
        id,
        scan_settings_ref,
        cv_param,
        user_param,
        referenceable_param_group_ref,
        component_list,
        software_ref,
    }
}

/// <componentList>
#[inline]
fn parse_component_list(ctx: &ParseCtx<'_>, instrument_id: u32) -> Option<ComponentList> {
    let component_list_ids = ids_for_parent(ctx, instrument_id, TagId::ComponentList);
    let component_list_id = component_list_ids.first().copied().unwrap_or(0);

    let primary_parent = if component_list_id != 0 {
        component_list_id
    } else {
        instrument_id
    };

    let mut source_ids = ids_for_parent(ctx, primary_parent, TagId::ComponentSource);
    let mut analyzer_ids = ids_for_parent(ctx, primary_parent, TagId::ComponentAnalyzer);
    let mut detector_ids = ids_for_parent(ctx, primary_parent, TagId::ComponentDetector);

    if source_ids.is_empty()
        && analyzer_ids.is_empty()
        && detector_ids.is_empty()
        && primary_parent != instrument_id
    {
        source_ids = ids_for_parent(ctx, instrument_id, TagId::ComponentSource);
        analyzer_ids = ids_for_parent(ctx, instrument_id, TagId::ComponentAnalyzer);
        detector_ids = ids_for_parent(ctx, instrument_id, TagId::ComponentDetector);
    }

    if source_ids.is_empty() && analyzer_ids.is_empty() && detector_ids.is_empty() {
        return None;
    }

    let mut source = Vec::with_capacity(source_ids.len());
    for id in source_ids {
        source.push(parse_source(ctx, id));
    }

    let mut analyzer = Vec::with_capacity(analyzer_ids.len());
    for id in analyzer_ids {
        analyzer.push(parse_analyzer(ctx, id));
    }

    let mut detector = Vec::with_capacity(detector_ids.len());
    for id in detector_ids {
        detector.push(parse_detector(ctx, id));
    }

    Some(ComponentList {
        count: Some(source.len() + analyzer.len() + detector.len()),
        source,
        analyzer,
        detector,
    })
}

/// <source>
#[inline]
fn parse_source(ctx: &ParseCtx<'_>, source_id: u32) -> Source {
    let rows = rows_for_owner(ctx.owner_rows, source_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(ctx.owner_rows, ctx.child_index, source_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, source_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    Source {
        order,
        referenceable_param_group_ref,
        cv_param,
        user_param,
    }
}

/// <analyzer>
#[inline]
fn parse_analyzer(ctx: &ParseCtx<'_>, analyzer_id: u32) -> Analyzer {
    let rows = rows_for_owner(ctx.owner_rows, analyzer_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(ctx.owner_rows, ctx.child_index, analyzer_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, analyzer_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    Analyzer {
        order,
        referenceable_param_group_ref,
        cv_param,
        user_param,
    }
}

/// <detector>
#[inline]
fn parse_detector(ctx: &ParseCtx<'_>, detector_id: u32) -> Detector {
    let rows = rows_for_owner(ctx.owner_rows, detector_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(ctx.owner_rows, ctx.child_index, detector_id);

    let child_meta = child_params_for_parent(ctx.owner_rows, ctx.child_index, detector_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta: Vec<&Metadatum> = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    Detector {
        order,
        referenceable_param_group_ref,
        cv_param,
        user_param,
    }
}

/// <instrumentSoftwareRef> / <softwareRef>
#[inline]
fn parse_instrument_software_ref(
    ctx: &ParseCtx<'_>,
    instrument_id: u32,
) -> Option<InstrumentSoftwareRef> {
    let software_ref_ids = ids_for_parent(ctx, instrument_id, TagId::SoftwareRef);
    let software_ref_id = software_ref_ids.first().copied().unwrap_or(0);
    if software_ref_id == 0 {
        return None;
    }

    let rows = rows_for_owner(ctx.owner_rows, software_ref_id);
    let r#ref = b000_attr_text(rows, ACC_ATTR_REF)?;
    (!r#ref.is_empty()).then(|| InstrumentSoftwareRef { r#ref })
}

/// <referenceableParamGroupRef>
#[inline]
fn parse_referenceable_param_group_refs(
    owner_rows: &HashMap<u32, Vec<&Metadatum>>,
    child_index: &ChildIndex,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    let ref_ids = unique_ids(child_index.ids(parent_id, TagId::ReferenceableParamGroupRef));
    if ref_ids.is_empty() {
        return Vec::new();
    }

    let mut out: Vec<ReferenceableParamGroupRef> = Vec::with_capacity(ref_ids.len());
    for ref_id in ref_ids {
        let ref_rows = rows_for_owner(owner_rows, ref_id);
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                out.push(ReferenceableParamGroupRef { r#ref: r });
            }
        }
    }

    out
}
