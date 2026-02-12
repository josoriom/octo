use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::b000_attr_text,
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

#[inline]
pub fn parse_instrument_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<InstrumentList> {
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);
        if list_id.is_none() && m.tag_id == TagId::InstrumentConfigurationList {
            list_id = Some(m.id);
        }
    }

    let instrument_ids = if let Some(list_id) = list_id {
        let ids = children_lookup.ids_for(metadata, list_id, TagId::Instrument);
        if ids.is_empty() {
            ChildrenLookup::all_ids(metadata, TagId::Instrument)
        } else {
            ids
        }
    } else {
        ChildrenLookup::all_ids(metadata, TagId::Instrument)
    };

    if instrument_ids.is_empty() {
        return None;
    }

    let mut instrument = Vec::with_capacity(instrument_ids.len());
    for id in instrument_ids {
        instrument.push(parse_instrument(metadata, children_lookup, &owner_rows, id));
    }

    Some(InstrumentList {
        count: Some(instrument.len()),
        instrument,
    })
}

#[inline]
fn parse_instrument<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    instrument_id: u32,
) -> Instrument {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, instrument_id);

    let id = b000_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();

    let scan_settings_ref = b000_attr_text(rows, ACC_ATTR_SCAN_SETTINGS_REF)
        .filter(|s| !s.is_empty())
        .map(|s| ScanSettingsRef { r#ref: s });

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(metadata, children_lookup, owner_rows, instrument_id);

    let child_meta = children_lookup.param_rows(metadata, owner_rows, instrument_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
        params_meta.extend_from_slice(rows);
        params_meta.extend(child_meta);
        parse_cv_and_user_params(&params_meta)
    };

    let component_list = parse_component_list(metadata, children_lookup, owner_rows, instrument_id);
    let software_ref =
        parse_instrument_software_ref(metadata, children_lookup, owner_rows, instrument_id);

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

#[inline]
fn parse_component_list<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    instrument_id: u32,
) -> Option<ComponentList> {
    let component_list_id = children_lookup
        .ids_for(metadata, instrument_id, TagId::ComponentList)
        .first()
        .copied()
        .unwrap_or(0);

    let primary_parent = if component_list_id != 0 {
        component_list_id
    } else {
        instrument_id
    };

    let mut source_ids = children_lookup.ids_for(metadata, primary_parent, TagId::ComponentSource);
    let mut analyzer_ids =
        children_lookup.ids_for(metadata, primary_parent, TagId::ComponentAnalyzer);
    let mut detector_ids =
        children_lookup.ids_for(metadata, primary_parent, TagId::ComponentDetector);

    if source_ids.is_empty()
        && analyzer_ids.is_empty()
        && detector_ids.is_empty()
        && primary_parent != instrument_id
    {
        source_ids = children_lookup.ids_for(metadata, instrument_id, TagId::ComponentSource);
        analyzer_ids = children_lookup.ids_for(metadata, instrument_id, TagId::ComponentAnalyzer);
        detector_ids = children_lookup.ids_for(metadata, instrument_id, TagId::ComponentDetector);
    }

    if source_ids.is_empty() && analyzer_ids.is_empty() && detector_ids.is_empty() {
        return None;
    }

    let mut source = Vec::with_capacity(source_ids.len());
    for id in source_ids {
        source.push(parse_source(metadata, children_lookup, owner_rows, id));
    }

    let mut analyzer = Vec::with_capacity(analyzer_ids.len());
    for id in analyzer_ids {
        analyzer.push(parse_analyzer(metadata, children_lookup, owner_rows, id));
    }

    let mut detector = Vec::with_capacity(detector_ids.len());
    for id in detector_ids {
        detector.push(parse_detector(metadata, children_lookup, owner_rows, id));
    }

    Some(ComponentList {
        count: Some(source.len() + analyzer.len() + detector.len()),
        source,
        analyzer,
        detector,
    })
}

#[inline]
fn parse_source<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    source_id: u32,
) -> Source {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, source_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(metadata, children_lookup, owner_rows, source_id);

    let child_meta = children_lookup.param_rows(metadata, owner_rows, source_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
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

#[inline]
fn parse_analyzer<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    analyzer_id: u32,
) -> Analyzer {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, analyzer_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(metadata, children_lookup, owner_rows, analyzer_id);

    let child_meta = children_lookup.param_rows(metadata, owner_rows, analyzer_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
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

#[inline]
fn parse_detector<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    detector_id: u32,
) -> Detector {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, detector_id);

    let order = b000_attr_text(rows, ACC_ATTR_ORDER).and_then(|s| s.parse::<u32>().ok());

    let referenceable_param_group_ref =
        parse_referenceable_param_group_refs(metadata, children_lookup, owner_rows, detector_id);

    let child_meta = children_lookup.param_rows(metadata, owner_rows, detector_id);

    let (cv_param, user_param) = if child_meta.is_empty() {
        parse_cv_and_user_params(rows)
    } else {
        let mut params_meta = Vec::with_capacity(rows.len() + child_meta.len());
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

#[inline]
fn parse_instrument_software_ref<'m>(
    metadata: &[&'m Metadatum],
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows<'m>,
    instrument_id: u32,
) -> Option<InstrumentSoftwareRef> {
    let software_ref_id = children_lookup
        .ids_for(metadata, instrument_id, TagId::SoftwareRef)
        .first()
        .copied()
        .unwrap_or(0);

    if software_ref_id == 0 {
        return None;
    }

    let rows = ChildrenLookup::rows_for_owner(owner_rows, software_ref_id);
    let r#ref = b000_attr_text(rows, ACC_ATTR_REF)?;
    (!r#ref.is_empty()).then(|| InstrumentSoftwareRef { r#ref })
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
        if let Some(r) = b000_attr_text(ref_rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                out.push(ReferenceableParamGroupRef { r#ref: r });
            }
        }
    }
    out
}
