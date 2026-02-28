use crate::{
    CvParam, UserParam,
    b64::{
        attr_meta::{ACC_ATTR_ID, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SCAN_SETTINGS_REF},
        utilities::{
            children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
            common::get_attr_text,
            parse_cv_and_user_params,
        },
    },
    decoder::decode::Metadatum,
    mzml::{
        schema::TagId,
        structs::{
            Analyzer, ComponentList, Detector, Instrument, InstrumentList, InstrumentSoftwareRef,
            ReferenceableParamGroupRef, ScanSettingsRef, Source,
        },
    },
};

pub(crate) fn parse_instrument_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<InstrumentList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
    }

    let list_id = children_lookup
        .all_ids(TagId::InstrumentConfigurationList)
        .first()
        .copied();

    let instrument_ids: &[u32] = list_id
        .map(|id| children_lookup.ids_for(id, TagId::Instrument))
        .filter(|ids| !ids.is_empty())
        .unwrap_or_else(|| children_lookup.all_ids(TagId::Instrument));

    if instrument_ids.is_empty() {
        return None;
    }

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let instrument = instrument_ids
        .iter()
        .map(|&instrument_id| {
            parse_instrument(
                children_lookup,
                &owner_rows,
                instrument_id,
                policy,
                &mut param_buffer,
            )
        })
        .collect::<Vec<_>>();

    Some(InstrumentList {
        count: Some(instrument.len()),
        instrument,
    })
}

fn parse_instrument<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    instrument_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Instrument {
    let rows = owner_rows.get(instrument_id);

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, instrument_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    Instrument {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        scan_settings_ref: get_attr_text(rows, ACC_ATTR_SCAN_SETTINGS_REF)
            .filter(|s| !s.is_empty())
            .map(|s| ScanSettingsRef { r#ref: s }),
        cv_param,
        user_param,
        referenceable_param_group_ref: parse_param_group_refs(
            children_lookup,
            owner_rows,
            instrument_id,
        ),
        component_list: parse_component_list(
            children_lookup,
            owner_rows,
            instrument_id,
            policy,
            param_buffer,
        ),
        software_ref: parse_software_ref(children_lookup, owner_rows, instrument_id),
    }
}

struct ComponentData {
    order: Option<u32>,
    referenceable_param_group_ref: Vec<ReferenceableParamGroupRef>,
    cv_param: Vec<CvParam>,
    user_param: Vec<UserParam>,
}

fn parse_component_data<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    component_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> ComponentData {
    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, component_id, policy, param_buffer);
    let (cv_param, user_param) = parse_cv_and_user_params(param_buffer);

    ComponentData {
        order: get_attr_text(owner_rows.get(component_id), ACC_ATTR_ORDER)
            .and_then(|s| s.parse().ok()),
        referenceable_param_group_ref: parse_param_group_refs(
            children_lookup,
            owner_rows,
            component_id,
        ),
        cv_param,
        user_param,
    }
}

fn parse_component_list<'a, P: MetadataPolicy>(
    children_lookup: &ChildrenLookup,
    owner_rows: &'a OwnerRows<'a>,
    instrument_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<ComponentList> {
    let component_root_id = children_lookup
        .ids_for(instrument_id, TagId::ComponentList)
        .first()
        .copied()
        .unwrap_or(instrument_id);

    let source_ids = children_lookup.ids_for(component_root_id, TagId::ComponentSource);
    let analyzer_ids = children_lookup.ids_for(component_root_id, TagId::ComponentAnalyzer);
    let detector_ids = children_lookup.ids_for(component_root_id, TagId::ComponentDetector);

    if source_ids.is_empty() && analyzer_ids.is_empty() && detector_ids.is_empty() {
        return None;
    }

    let source: Vec<Source> = source_ids
        .iter()
        .map(|&id| {
            let ComponentData {
                order,
                referenceable_param_group_ref,
                cv_param,
                user_param,
            } = parse_component_data(children_lookup, owner_rows, id, policy, param_buffer);
            Source {
                order,
                referenceable_param_group_ref,
                cv_param,
                user_param,
            }
        })
        .collect();

    let analyzer: Vec<Analyzer> = analyzer_ids
        .iter()
        .map(|&id| {
            let ComponentData {
                order,
                referenceable_param_group_ref,
                cv_param,
                user_param,
            } = parse_component_data(children_lookup, owner_rows, id, policy, param_buffer);
            Analyzer {
                order,
                referenceable_param_group_ref,
                cv_param,
                user_param,
            }
        })
        .collect();

    let detector: Vec<Detector> = detector_ids
        .iter()
        .map(|&id| {
            let ComponentData {
                order,
                referenceable_param_group_ref,
                cv_param,
                user_param,
            } = parse_component_data(children_lookup, owner_rows, id, policy, param_buffer);
            Detector {
                order,
                referenceable_param_group_ref,
                cv_param,
                user_param,
            }
        })
        .collect();

    Some(ComponentList {
        count: Some(source.len() + analyzer.len() + detector.len()),
        source,
        analyzer,
        detector,
    })
}

fn parse_software_ref(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    instrument_id: u32,
) -> Option<InstrumentSoftwareRef> {
    children_lookup
        .ids_for(instrument_id, TagId::SoftwareRef)
        .first()
        .and_then(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF)
                .map(|r| InstrumentSoftwareRef { r#ref: r })
        })
}

fn parse_param_group_refs(
    children_lookup: &ChildrenLookup,
    owner_rows: &OwnerRows,
    parent_id: u32,
) -> Vec<ReferenceableParamGroupRef> {
    children_lookup
        .ids_for(parent_id, TagId::ReferenceableParamGroupRef)
        .iter()
        .filter_map(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF)
                .map(|r| ReferenceableParamGroupRef { r#ref: r })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::decode::MetadatumValue;

    fn make_metadatum(id: u32, parent_id: u32, tag_id: TagId) -> Metadatum {
        Metadatum {
            item_index: 0,
            id,
            parent_id,
            tag_id,
            accession: None,
            unit_accession: None,
            value: MetadatumValue::Empty,
        }
    }

    #[test]
    fn parse_instrument_list_returns_none_when_no_instruments_in_metadata() {
        let metadata: Vec<Metadatum> = vec![make_metadatum(1, 0, TagId::SpectrumList)];
        let refs: Vec<&Metadatum> = metadata.iter().collect();
        let lookup = ChildrenLookup::new(&metadata);
        let policy = DefaultMetadataPolicy;
        assert!(parse_instrument_list(&refs, &lookup, &policy).is_none());
    }

    #[test]
    fn parse_component_list_returns_none_when_no_components_present() {
        let metadata: Vec<Metadatum> = vec![make_metadatum(1, 0, TagId::Instrument)];
        let mut owner_rows = OwnerRows::with_capacity(1);
        for m in &metadata {
            owner_rows.insert(m.id, m);
        }
        let lookup = ChildrenLookup::new(&metadata);
        let policy = DefaultMetadataPolicy;
        let mut buf = Vec::new();

        let result = parse_component_list(&lookup, &owner_rows, 1, &policy, &mut buf);
        assert!(result.is_none());
    }

    #[test]
    fn parse_component_data_reads_order_attribute() {
        use crate::b64::attr_meta::ACC_ATTR_ORDER;
        let order_row = Metadatum {
            item_index: 0,
            id: 2,
            parent_id: 1,
            tag_id: TagId::ComponentSource,
            accession: Some({
                use core::fmt::Write;
                let mut s = String::from("B000:");
                write!(&mut s, "{:07}", ACC_ATTR_ORDER.raw()).unwrap();
                s
            }),
            unit_accession: None,
            value: MetadatumValue::Number(3.0),
        };
        let metadata = vec![make_metadatum(2, 1, TagId::ComponentSource), order_row];
        let mut owner_rows = OwnerRows::with_capacity(2);
        for m in &metadata {
            owner_rows.insert(m.id, m);
        }
        let lookup = ChildrenLookup::new(&metadata);
        let policy = DefaultMetadataPolicy;
        let mut buf = Vec::new();

        let data = parse_component_data(&lookup, &owner_rows, 2, &policy, &mut buf);
        assert_eq!(data.order, Some(3));
    }

    #[test]
    fn component_list_counts_all_component_types() {
        let metadata = vec![
            make_metadatum(1, 0, TagId::Instrument),
            make_metadatum(2, 1, TagId::ComponentList),
            make_metadatum(3, 2, TagId::ComponentSource),
            make_metadatum(4, 2, TagId::ComponentAnalyzer),
            make_metadatum(5, 2, TagId::ComponentAnalyzer),
            make_metadatum(6, 2, TagId::ComponentDetector),
        ];
        let mut owner_rows = OwnerRows::with_capacity(6);
        for m in &metadata {
            owner_rows.insert(m.id, m);
        }
        let lookup = ChildrenLookup::new(&metadata);
        let policy = DefaultMetadataPolicy;
        let mut buf = Vec::new();

        let list = parse_component_list(&lookup, &owner_rows, 1, &policy, &mut buf).unwrap();
        assert_eq!(list.source.len(), 1);
        assert_eq!(list.analyzer.len(), 2);
        assert_eq!(list.detector.len(), 1);
        assert_eq!(list.count, Some(4));
    }

    use crate::b64::utilities::children_lookup::ChildrenLookup;
    use crate::b64::utilities::children_lookup::DefaultMetadataPolicy;
}
