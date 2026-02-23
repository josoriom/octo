use crate::{
    Header,
    b64::utilities::{
        children_lookup::{ChildrenLookup, DefaultMetadataPolicy, OwnerRows},
        common::{get_attr_text, parse_accession_tail_str, read_u32_le_at, read_u64_le_at, take},
        container_builder::FilterType,
        container_view::{BlockProcessor, ContainerView, DefaultProcessor},
        parse_chromatogram_list::parse_chromatogram_list,
        parse_cv_and_user_params, parse_cv_list, parse_data_processing_list,
        parse_file_description::parse_file_description,
        parse_global_metadata::parse_global_metadata,
        parse_header, parse_instrument_list, parse_metadata,
        parse_referenceable_param_group_list::parse_referenceable_param_group_list,
        parse_sample_list, parse_scan_settings_list, parse_software_list,
        parse_spectrum_list::parse_spectrum_list,
    },
    mzml::{attr_meta::*, schema::TagId, structs::*},
};

pub const A0_ENTRY_SIZE: u64 = 16;
pub const A1_ENTRY_SIZE: u64 = 32;

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

    let (mut spec_binaries, mut chrom_binaries) = parse_binaries(bytes, header)?;
    attach_binaries(&mut run, &mut spec_binaries, &mut chrom_binaries);

    Ok(run)
}

#[inline]
fn parse_binaries(
    bytes: &[u8],
    h: &Header,
) -> Result<
    (
        Vec<Option<Vec<(u32, ArrayData)>>>,
        Vec<Option<Vec<(u32, ArrayData)>>>,
    ),
    String,
> {
    let spec_a1 = parse_a1(bytes, h.off_spec_arrayrefs, h.len_spec_arrayrefs)?;
    let chrom_a1 = parse_a1(bytes, h.off_chrom_arrayrefs, h.len_chrom_arrayrefs)?;

    let array_filter = FilterType::try_from(h.array_filter)?;

    let mut s_view = ContainerView::new(
        slice_at(bytes, h.off_container_spect, h.len_container_spect, "spec")?,
        h.block_count_spect,
        h.compression_level,
        array_filter,
        "spec",
        DefaultProcessor,
    )?;

    let mut c_view = ContainerView::new(
        slice_at(bytes, h.off_container_chrom, h.len_container_chrom, "chrom")?,
        h.block_count_chrom,
        h.compression_level,
        array_filter,
        "chrom",
        DefaultProcessor,
    )?;

    let mut s_data = Vec::with_capacity(h.spectrum_count as usize);
    for entry in parse_a0(
        bytes,
        h.off_spec_entries,
        h.len_spec_entries,
        h.spectrum_count,
    )? {
        s_data.push(Some(extract_arrays(&mut s_view, &spec_a1, &entry)));
    }

    let mut c_data = Vec::with_capacity(h.chrom_count as usize);
    for entry in parse_a0(
        bytes,
        h.off_chrom_entries,
        h.len_chrom_entries,
        h.chrom_count,
    )? {
        c_data.push(Some(extract_arrays(&mut c_view, &chrom_a1, &entry)));
    }

    Ok((s_data, c_data))
}

#[inline]
fn extract_arrays<P: BlockProcessor>(
    view: &mut ContainerView<'_, P>,
    a1: &[A1Ref],
    entry: &A0Entry,
) -> Vec<(u32, ArrayData)> {
    let start = entry.a1_start as usize;
    let end = start + entry.a1_count as usize;

    if end > a1.len() {
        return Vec::new();
    }

    a1[start..end]
        .iter()
        .filter_map(|r| {
            let (size, numeric_type) = dtype_to_layout(r.dtype).ok()?;
            let raw = view
                .get_item_from_block(r.block_id, r.element_off, r.len_elements, size, "")
                .ok()?;
            Some((r.array_type, bytes_to_data(raw, numeric_type)))
        })
        .collect()
}

#[inline]
fn attach_binaries(
    run: &mut Run,
    spec_binaries: &mut [Option<Vec<(u32, ArrayData)>>],
    chrom_binaries: &mut [Option<Vec<(u32, ArrayData)>>],
) {
    if let Some(spectrum_list) = run.spectrum_list.as_mut() {
        for (index, spectrum) in spectrum_list.spectra.iter_mut().enumerate() {
            if let Some(arrays) = spec_binaries.get_mut(index).and_then(|slot| slot.take()) {
                if let Some(bdal) = spectrum.binary_data_array_list.as_mut() {
                    bind_arrays(bdal, arrays);
                }
            }
        }
    }
    if let Some(chromatogram_list) = run.chromatogram_list.as_mut() {
        for (index, chromatogram) in chromatogram_list.chromatograms.iter_mut().enumerate() {
            if let Some(arrays) = chrom_binaries.get_mut(index).and_then(|slot| slot.take()) {
                if let Some(bdal) = chromatogram.binary_data_array_list.as_mut() {
                    bind_arrays(bdal, arrays);
                }
            }
        }
    }
}

#[inline]
fn bind_arrays(list: &mut BinaryDataArrayList, arrays: Vec<(u32, ArrayData)>) {
    for (kind, data) in arrays {
        let found = list
            .binary_data_arrays
            .iter_mut()
            .find(|b| bda_matches(b, kind));

        if let Some(bda) = found {
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
        } else {
            panic!(
                "DECODE ERROR: Could not find BinaryDataArray for array type 1000{:03}. Verify encoder is preserving array type CV params.",
                kind % 1000
            );
        }
    }
    list.count = Some(list.binary_data_arrays.len());
}

#[inline]
fn sync_numeric_meta(bda: &mut BinaryDataArray, numeric_type: NumericType) {
    let target_accession = match numeric_type {
        NumericType::Float16 => 1_000_520,
        NumericType::Float32 => 1_000_521,
        NumericType::Float64 => 1_000_523,
        NumericType::Int16 => 1_000_518,
        NumericType::Int32 => 1_000_519,
        NumericType::Int64 => 1_000_522,
    };
    bda.cv_params.retain(|p| {
        !is_numeric_acc(parse_accession_tail_str(
            p.accession.as_deref().unwrap_or(""),
        ))
    });
    bda.cv_params.push(ms_param(target_accession));
    bda.numeric_type = Some(numeric_type);
}

#[inline]
fn bda_matches(bda: &BinaryDataArray, kind: u32) -> bool {
    bda.cv_params.iter().any(|p| {
        let accession = p.accession.as_deref().unwrap_or("");
        parse_accession_tail_str(accession) == kind
    })
}

#[inline]
fn is_numeric_acc(tail: u32) -> bool {
    matches!(
        tail,
        1000520 | 1000521 | 1000523 | 1000518 | 1000519 | 1000522
    )
}

#[inline]
fn ms_param(tail: u32) -> CvParam {
    let name = match tail {
        1000521 => "32-bit float",
        1000523 => "64-bit float",
        1000519 => "32-bit integer",
        1000522 => "64-bit integer",
        _ => "numeric",
    };
    CvParam {
        cv_ref: Some("MS".into()),
        accession: Some(format!("MS:{:07}", tail)),
        name: name.into(),
        ..Default::default()
    }
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
pub fn slice_at<'a>(bytes: &'a [u8], off: u64, len: u64, f: &str) -> Result<&'a [u8], String> {
    let (offset, length) = (off as usize, len as usize);
    bytes
        .get(offset..offset + length)
        .ok_or_else(|| format!("{f}: range error"))
}

#[inline]
fn dtype_to_layout(dtype: u8) -> Result<(usize, NumericType), String> {
    match dtype {
        1 => Ok((8, NumericType::Float64)),
        2 => Ok((4, NumericType::Float32)),
        3 => Ok((2, NumericType::Float16)),
        4 => Ok((2, NumericType::Int16)),
        5 => Ok((4, NumericType::Int32)),
        6 => Ok((8, NumericType::Int64)),
        _ => Err(format!("invalid dtype {dtype}")),
    }
}

#[inline]
fn bytes_to_data(raw: &[u8], numeric_type: NumericType) -> ArrayData {
    match numeric_type {
        NumericType::Float64 => ArrayData::F64(to_vec(raw)),
        NumericType::Float32 => ArrayData::F32(to_vec(raw)),
        NumericType::Float16 => ArrayData::F16(to_vec(raw)),
        NumericType::Int64 => ArrayData::I64(to_vec(raw)),
        NumericType::Int32 => ArrayData::I32(to_vec(raw)),
        NumericType::Int16 => ArrayData::I16(to_vec(raw)),
    }
}

#[inline]
fn to_vec<T>(raw: &[u8]) -> Vec<T> {
    let element_count = raw.len() / std::mem::size_of::<T>();
    let mut out = Vec::with_capacity(element_count);
    unsafe {
        out.set_len(element_count);
        std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
    }
    out
}

struct A0Entry {
    a1_start: u64,
    a1_count: u64,
}

struct A1Ref {
    array_type: u32,
    dtype: u8,
    block_id: u32,
    element_off: u64,
    len_elements: u64,
}

#[inline]
fn parse_a0(bytes: &[u8], off: u64, len: u64, count: u32) -> Result<Vec<A0Entry>, String> {
    let raw = slice_at(bytes, off, len, "A0")?;
    let mut position = 0;
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let a1_start = read_u64_le_at(raw, &mut position, "a1_start")?;
        let a1_count = read_u64_le_at(raw, &mut position, "a1_count")?;
        out.push(A0Entry { a1_start, a1_count });
    }
    Ok(out)
}

#[inline]
fn parse_a1(bytes: &[u8], off: u64, len: u64) -> Result<Vec<A1Ref>, String> {
    let raw = slice_at(bytes, off, len, "A1")?;
    let mut position = 0;
    let entry_count = (len / A1_ENTRY_SIZE) as usize;
    let mut out = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let element_off = read_u64_le_at(raw, &mut position, "e_off")?;
        let len_elements = read_u64_le_at(raw, &mut position, "len_e")?;
        let block_id = read_u32_le_at(raw, &mut position, "blk")?;
        let array_type = read_u32_le_at(raw, &mut position, "type")?;
        let dtype = take(raw, &mut position, 1, "dt")?[0];
        let _ = take(raw, &mut position, 7, "pad")?;
        out.push(A1Ref {
            array_type,
            dtype,
            block_id,
            element_off,
            len_elements,
        });
    }
    Ok(out)
}

#[derive(Clone, Debug)]
enum ArrayData {
    F64(Vec<f64>),
    F32(Vec<f32>),
    F16(Vec<u16>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetadatumValue {
    Number(f64),
    Text(String),
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Metadatum {
    pub item_index: u32,
    pub id: u32,
    pub parent_id: u32,
    pub tag_id: TagId,
    pub accession: Option<String>,
    pub unit_accession: Option<String>,
    pub value: MetadatumValue,
}
