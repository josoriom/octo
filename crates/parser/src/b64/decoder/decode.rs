use crate::{
    Header,
    b64::{
        attr_meta::{
            ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
            ACC_ATTR_ID,
            ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
            ACC_ATTR_REF,
            ACC_ATTR_SAMPLE_REF,
            ACC_ATTR_START_TIME_STAMP,
            parse_accession_tail, // ← canonical, returns AccessionTail
        },
        encoder::utilities::FilterType,
        utilities::{
            children_lookup::{ChildrenLookup, DefaultMetadataPolicy, OwnerRows},
            common::get_attr_text,
            container_view::{ArrayData, BinaryStore, BinaryStoreConfig},
            parse_chromatogram_list, parse_cv_and_user_params, parse_cv_list,
            parse_data_processing_list, parse_file_description,
            parse_global_metadata::parse_global_metadata,
            parse_header, parse_instrument_list, parse_metadata,
            parse_referenceable_param_group_list, parse_sample_list, parse_scan_settings_list,
            parse_software_list, parse_spectrum_list,
        },
    },
    mzml::{schema::TagId, structs::*},
};

#[inline]
pub fn decode(bytes: &[u8]) -> Result<MzML, String> {
    let header = parse_header(bytes)?;
    let global_meta = parse_global_section(bytes, &header)?;
    let lookup = ChildrenLookup::new(&global_meta);
    let meta_refs: Vec<&Metadatum> = global_meta.iter().collect();
    let policy = DefaultMetadataPolicy;

    Ok(MzML {
        cv_list: parse_cv_list(&meta_refs, &lookup),
        file_description: parse_file_description(&meta_refs, &lookup, &policy),
        referenceable_param_group_list: parse_referenceable_param_group_list(
            &meta_refs, &lookup, &policy,
        ),
        sample_list: parse_sample_list(&meta_refs, &lookup, &policy),
        instrument_list: parse_instrument_list(&meta_refs, &lookup, &policy),
        software_list: parse_software_list(&meta_refs, &lookup, &policy),
        data_processing_list: parse_data_processing_list(&meta_refs, &lookup, &policy),
        scan_settings_list: parse_scan_settings_list(&meta_refs, &lookup, &policy),
        run: parse_run(bytes, &header, &global_meta, &policy)?,
    })
}

#[inline]
fn parse_run(
    bytes: &[u8],
    header: &Header,
    global_meta: &[Metadatum],
    policy: &DefaultMetadataPolicy,
) -> Result<Run, String> {
    let mut owner_rows = OwnerRows::with_capacity(global_meta.len());
    for m in global_meta {
        owner_rows.insert(m.id, m);
    }
    let children_lookup = ChildrenLookup::new(global_meta);

    let run_id = children_lookup
        .all_ids(TagId::Run)
        .first()
        .copied()
        .unwrap_or(0);

    let rows = owner_rows.get(run_id);

    let mut param_buffer: Vec<&Metadatum> = Vec::new();
    children_lookup.get_param_rows_into(&owner_rows, run_id, policy, &mut param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(&param_buffer);

    let spec_meta = parse_metadata_section(bytes, header, true)?;
    let chrom_meta = parse_metadata_section(bytes, header, false)?;

    let spec_refs: Vec<&Metadatum> = spec_meta.iter().collect();
    let chrom_refs: Vec<&Metadatum> = chrom_meta.iter().collect();

    let mut run = Run {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        start_time_stamp: get_attr_text(rows, ACC_ATTR_START_TIME_STAMP).filter(|s| !s.is_empty()),
        default_instrument_configuration_ref: get_attr_text(
            rows,
            ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
        )
        .or_else(|| get_attr_text(rows, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF)),
        sample_ref: get_attr_text(rows, ACC_ATTR_SAMPLE_REF),
        cv_params,
        user_params,
        source_file_ref_list: parse_run_source_file_refs(&owner_rows, &children_lookup, run_id),
        spectrum_list: parse_spectrum_list(&spec_refs, &ChildrenLookup::new(&spec_meta), policy),
        chromatogram_list: parse_chromatogram_list(
            &chrom_refs,
            &ChildrenLookup::new(&chrom_meta),
            policy,
        ),
        ..Default::default()
    };

    // ── binary data ───────────────────────────────────────────────────────────
    // BinaryStore owns the full extraction pipeline; decode.rs sees only
    // pre-decoded slots that are consumed once via `take`.
    let filter = FilterType::try_from(header.array_filter)?;

    let mut spec_store = BinaryStore::build(
        slice_at(
            bytes,
            header.off_container_spect,
            header.len_container_spect,
            "spec",
        )?,
        slice_at(
            bytes,
            header.off_spec_arrayrefs,
            header.len_spec_arrayrefs,
            "A1-spec",
        )?,
        slice_at(
            bytes,
            header.off_spec_entries,
            header.len_spec_entries,
            "A0-spec",
        )?,
        BinaryStoreConfig {
            block_count: header.block_count_spect,
            item_count: header.spectrum_count,
            compression_level: header.compression_level,
            filter,
            context_label: "spec",
        },
    )?;

    let mut chrom_store = BinaryStore::build(
        slice_at(
            bytes,
            header.off_container_chrom,
            header.len_container_chrom,
            "chrom",
        )?,
        slice_at(
            bytes,
            header.off_chrom_arrayrefs,
            header.len_chrom_arrayrefs,
            "A1-chrom",
        )?,
        slice_at(
            bytes,
            header.off_chrom_entries,
            header.len_chrom_entries,
            "A0-chrom",
        )?,
        BinaryStoreConfig {
            block_count: header.block_count_chrom,
            item_count: header.chrom_count,
            compression_level: header.compression_level,
            filter,
            context_label: "chrom",
        },
    )?;

    attach_binaries(&mut run, &mut spec_store, &mut chrom_store);
    Ok(run)
}

// ── Binary attachment ─────────────────────────────────────────────────────────

fn attach_binaries(run: &mut Run, spec: &mut BinaryStore, chrom: &mut BinaryStore) {
    if let Some(list) = run.spectrum_list.as_mut() {
        for (i, spectrum) in list.spectra.iter_mut().enumerate() {
            let Some(arrays) = spec.take(i) else { continue };
            if arrays.is_empty() {
                continue;
            }
            let bdal = spectrum
                .binary_data_array_list
                .get_or_insert_with(BinaryDataArrayList::default);
            bind_arrays(bdal, arrays);
        }
    }
    if let Some(list) = run.chromatogram_list.as_mut() {
        for (i, chromatogram) in list.chromatograms.iter_mut().enumerate() {
            let Some(arrays) = chrom.take(i) else {
                continue;
            };
            if arrays.is_empty() {
                continue;
            }
            let bdal = chromatogram
                .binary_data_array_list
                .get_or_insert_with(BinaryDataArrayList::default);
            bind_arrays(bdal, arrays);
        }
    }
}

// AFTER
fn bind_arrays(list: &mut BinaryDataArrayList, arrays: Vec<(u32, ArrayData)>) {
    for (kind, data) in arrays {
        let found = list
            .binary_data_arrays
            .iter_mut()
            .find(|b| bda_matches(b, kind));

        let bda = match found {
            Some(existing) => existing,
            None => {
                list.binary_data_arrays.push(make_bda_stub(kind));
                list.binary_data_arrays.last_mut().unwrap()
            }
        };

        let numeric_type = match data {
            ArrayData::F16(v) => {
                bda.binary = Some(BinaryData::F16(v));
                NumericType::Float16
            }
            ArrayData::F32(v) => {
                bda.binary = Some(BinaryData::F32(v));
                NumericType::Float32
            }
            ArrayData::F64(v) => {
                bda.binary = Some(BinaryData::F64(v));
                NumericType::Float64
            }
            ArrayData::I16(v) => {
                bda.binary = Some(BinaryData::I16(v));
                NumericType::Int16
            }
            ArrayData::I32(v) => {
                bda.binary = Some(BinaryData::I32(v));
                NumericType::Int32
            }
            ArrayData::I64(v) => {
                bda.binary = Some(BinaryData::I64(v));
                NumericType::Int64
            }
        };
        sync_numeric_meta(bda, numeric_type);
    }
    list.count = Some(list.binary_data_arrays.len());
}

fn make_bda_stub(array_type_accession: u32) -> BinaryDataArray {
    BinaryDataArray {
        cv_params: vec![CvParam {
            cv_ref: Some("MS".to_string()),
            accession: Some(format!("MS:{array_type_accession:07}")),
            name: String::new(),
            value: None,
            unit_cv_ref: None,
            unit_name: None,
            unit_accession: None,
        }],
        ..BinaryDataArray::default()
    }
}

// ── CV param sync ─────────────────────────────────────────────────────────────

#[inline]
fn sync_numeric_meta(bda: &mut BinaryDataArray, numeric_type: NumericType) {
    let target = match numeric_type {
        NumericType::Float16 => 1_000_520,
        NumericType::Float32 => 1_000_521,
        NumericType::Float64 => 1_000_523,
        NumericType::Int16 => 1_000_518,
        NumericType::Int32 => 1_000_519,
        NumericType::Int64 => 1_000_522,
    };
    // Remove any existing numeric-type CV params, then push the correct one.
    bda.cv_params
        .retain(|p| !is_numeric_acc(parse_accession_tail(p.accession.as_deref())));
    bda.cv_params.push(ms_numeric_param(target));
    bda.numeric_type = Some(numeric_type);
}

/// True when `tail` identifies one of the six MS numeric-type accessions.
#[inline]
fn is_numeric_acc(tail: crate::b64::attr_meta::AccessionTail) -> bool {
    matches!(
        tail.raw(),
        1_000_520 | 1_000_521 | 1_000_523 | 1_000_518 | 1_000_519 | 1_000_522
    )
}

/// True when `bda`'s CV params include `kind` as an array-type accession tail.
#[inline]
fn bda_matches(bda: &BinaryDataArray, kind: u32) -> bool {
    bda.cv_params
        .iter()
        .any(|p| parse_accession_tail(p.accession.as_deref()).raw() == kind)
}

#[inline]
fn ms_numeric_param(tail: u32) -> CvParam {
    let name = match tail {
        1_000_521 => "32-bit float",
        1_000_523 => "64-bit float",
        1_000_519 => "32-bit integer",
        1_000_522 => "64-bit integer",
        _ => "numeric",
    };
    CvParam {
        cv_ref: Some("MS".into()),
        accession: Some(format!("MS:{tail:07}")),
        name: name.into(),
        ..Default::default()
    }
}

// ── Section slicing and metadata helpers ──────────────────────────────────────

#[inline]
pub(crate) fn slice_at<'a>(
    bytes: &'a [u8],
    off: u64,
    len: u64,
    f: &str,
) -> Result<&'a [u8], String> {
    let (o, l) = (off as usize, len as usize);
    bytes
        .get(o..o + l)
        .ok_or_else(|| format!("{f}: range error"))
}

#[inline]
fn parse_global_section(bytes: &[u8], h: &Header) -> Result<Vec<Metadatum>, String> {
    parse_global_metadata(
        slice_at(bytes, h.off_global_meta, h.len_global_meta, "global")?,
        0,
        h.global_meta_count,
        h.global_meta_num_count,
        h.global_meta_str_count,
        h.compression_codec,
        h.global_meta_uncompressed_bytes,
    )
}

#[inline]
fn parse_metadata_section(
    bytes: &[u8],
    h: &Header,
    is_spec: bool,
) -> Result<Vec<Metadatum>, String> {
    let (off, len, count, n_count, s_count, uncompressed) = if is_spec {
        (
            h.off_spec_meta,
            h.len_spec_meta,
            h.spec_meta_count,
            h.spec_meta_num_count,
            h.spec_meta_str_count,
            h.spec_meta_uncompressed_bytes,
        )
    } else {
        (
            h.off_chrom_meta,
            h.len_chrom_meta,
            h.chrom_meta_count,
            h.chrom_meta_num_count,
            h.chrom_meta_str_count,
            h.chrom_meta_uncompressed_bytes,
        )
    };
    parse_metadata(
        slice_at(bytes, off, len, "meta")?,
        if is_spec {
            h.spectrum_count
        } else {
            h.chrom_count
        },
        count,
        n_count,
        s_count,
        h.compression_codec,
        uncompressed as usize,
    )
}

#[inline]
fn parse_run_source_file_refs(
    owner_rows: &OwnerRows,
    lookup: &ChildrenLookup,
    run_id: u32,
) -> Option<SourceFileRefList> {
    let list_id = lookup
        .ids_for(run_id, TagId::SourceFileRefList)
        .first()
        .copied()
        .or_else(|| lookup.all_ids(TagId::SourceFileRefList).first().copied())?;

    let refs: Vec<_> = lookup
        .ids_for(list_id, TagId::SourceFileRef)
        .iter()
        .filter_map(|&ref_id| {
            get_attr_text(owner_rows.get(ref_id), ACC_ATTR_REF).map(|r| SourceFileRef { r#ref: r })
        })
        .collect();

    (!refs.is_empty()).then(|| SourceFileRefList {
        count: Some(refs.len()),
        source_file_refs: refs,
    })
}

// ── Metadatum types (decoder-internal) ───────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MetadatumValue {
    Number(f64),
    Text(String),
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Metadatum {
    pub(crate) item_index: u32,
    pub(crate) id: u32,
    pub(crate) parent_id: u32,
    pub(crate) tag_id: TagId,
    pub(crate) accession: Option<String>,
    pub(crate) unit_accession: Option<String>,
    pub(crate) value: MetadatumValue,
}
