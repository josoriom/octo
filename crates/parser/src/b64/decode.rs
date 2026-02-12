use std::collections::HashSet;

use crate::{
    b64::utilities::{
        Header,
        children_lookup::{ChildrenLookup, OwnerRows},
        common::{b000_attr_text, parse_accession_tail, parse_accession_tail_str, take},
        container::ContainerView,
        parse_chromatogram_list, parse_cv_and_user_params, parse_cv_list,
        parse_data_processing_list,
        parse_file_description::parse_file_description,
        parse_global_metadata::parse_global_metadata,
        parse_header, parse_instrument_list, parse_metadata, parse_referenceable_param_group_list,
        parse_sample_list, parse_scan_settings_list, parse_software_list, parse_spectrum_list,
    },
    mzml::{attr_meta::*, schema::TagId, structs::*},
};

pub const A0_ENTRY_SIZE: u64 = 16;
pub const A1_ENTRY_SIZE: u64 = 32;

const ACC_32BIT_FLOAT: u32 = 1_000_521;
const ACC_64BIT_FLOAT: u32 = 1_000_523;
const ACC_16BIT_FLOAT: u32 = 1_000_520;
const ACC_16BIT_INTEGER: u32 = 1_000_518;
const ACC_32BIT_INTEGER: u32 = 1_000_519;
const ACC_64BIT_INTEGER: u32 = 1_000_522;

pub fn decode(bytes: &[u8]) -> Result<MzML, String> {
    let header = parse_header(bytes)?;
    let global_meta = parse_global_metadata_section(bytes, &header)?;
    let global_children_lookup = ChildrenLookup::new(&global_meta);

    let global_meta_ref: Vec<&Metadatum> = global_meta.iter().collect();

    let cv_list = parse_cv_list(&global_meta_ref, &global_children_lookup);

    Ok(MzML {
        cv_list,
        file_description: parse_file_description(&global_meta_ref, &global_children_lookup)
            .expect("missing <fileDescription> in global metadata"),
        referenceable_param_group_list: parse_referenceable_param_group_list(
            &global_meta_ref,
            &global_children_lookup,
        ),
        sample_list: parse_sample_list(&global_meta_ref, &global_children_lookup),
        instrument_list: parse_instrument_list(&global_meta_ref, &global_children_lookup),
        software_list: parse_software_list(&global_meta_ref, &global_children_lookup),
        data_processing_list: parse_data_processing_list(&global_meta_ref, &global_children_lookup),
        scan_settings_list: parse_scan_settings_list(&global_meta_ref, &global_children_lookup),
        run: parse_run(bytes, &header, &global_meta)?,
    })
}

#[inline]
fn parse_run(bytes: &[u8], header: &Header, global_meta: &[Metadatum]) -> Result<Run, String> {
    let spec_meta = parse_metadata(
        &bytes
            [header.off_spec_meta as usize..(header.off_spec_meta + header.len_spec_meta) as usize],
        header.spectrum_count,
        header.spec_meta_count,
        header.spec_meta_num_count,
        header.spec_meta_str_count,
        header.compression_codec,
        header.spec_meta_uncompressed_bytes as usize,
    )?;

    let chrom_meta = parse_metadata(
        &bytes[header.off_chrom_meta as usize
            ..(header.off_chrom_meta + header.len_chrom_meta) as usize],
        header.chrom_count,
        header.chrom_meta_count,
        header.chrom_meta_num_count,
        header.chrom_meta_str_count,
        header.compression_codec,
        header.chrom_meta_uncompressed_bytes as usize,
    )?;

    let run_children_lookup = ChildrenLookup::new(global_meta);

    let mut owner_rows: OwnerRows = OwnerRows::with_capacity(global_meta.len() / 2 + 1);
    for m in global_meta {
        owner_rows.entry(m.id).or_default().push(m);
    }

    let run_id = global_meta
        .iter()
        .find(|m| m.tag_id == TagId::Run)
        .map(|m| m.id)
        .unwrap_or(0);

    let run_rows = ChildrenLookup::rows_for_owner(&owner_rows, run_id);

    let id = b000_attr_text(run_rows, ACC_ATTR_ID).unwrap_or_default();
    let start_time_stamp =
        b000_attr_text(run_rows, ACC_ATTR_START_TIME_STAMP).filter(|s| !s.is_empty());

    let default_instrument_configuration_ref =
        b000_attr_text(run_rows, ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF)
            .or_else(|| b000_attr_text(run_rows, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF))
            .filter(|s| !s.is_empty());

    let sample_ref = b000_attr_text(run_rows, ACC_ATTR_SAMPLE_REF).filter(|s| !s.is_empty());

    let global_meta_ref: Vec<&Metadatum> = global_meta.iter().collect();

    let child_param_rows = run_children_lookup.param_rows(&global_meta_ref, &owner_rows, run_id);

    let mut params_meta = Vec::with_capacity(run_rows.len() + child_param_rows.len());
    params_meta.extend(run_rows.iter().copied());
    params_meta.extend(child_param_rows);

    let (cv_params, user_params) = parse_cv_and_user_params(&params_meta);

    let global_meta_ref: Vec<&Metadatum> = global_meta.iter().collect();

    let source_file_ref_list =
        parse_source_file_ref_list(&owner_rows, &run_children_lookup, &global_meta_ref, run_id);

    let spec_children_lookup = ChildrenLookup::new(&spec_meta);
    let chrom_children_lookup = ChildrenLookup::new(&chrom_meta);

    let spec_meta_ref: Vec<&Metadatum> = spec_meta.iter().collect();
    let chrom_meta_ref: Vec<&Metadatum> = chrom_meta.iter().collect();

    let spectrum_list = parse_spectrum_list(&spec_meta_ref, &spec_children_lookup);
    let chromatogram_list = parse_chromatogram_list(&chrom_meta_ref, &chrom_children_lookup);

    let (mut spectra_arrays, mut chrom_arrays) = parse_binaries(bytes, header)?;

    let mut run = Run {
        id,
        start_time_stamp,
        default_instrument_configuration_ref,
        sample_ref,
        cv_params,
        user_params,
        source_file_ref_list,
        spectrum_list,
        chromatogram_list,
        ..Default::default()
    };

    attach_pairs_to_run_lists(&mut run, &mut spectra_arrays, &mut chrom_arrays);

    Ok(run)
}

#[inline]
pub fn slice_at<'a>(
    bytes: &'a [u8],
    off: u64,
    len: u64,
    field: &'static str,
) -> Result<&'a [u8], String> {
    let off = usize::try_from(off).map_err(|_| format!("{field}: offset overflow"))?;
    let len = usize::try_from(len).map_err(|_| format!("{field}: len overflow"))?;
    let end = off
        .checked_add(len)
        .ok_or_else(|| format!("{field}: range overflow"))?;
    if end > bytes.len() {
        return Err(format!(
            "{field}: out of range (off={off}, len={len}, file_len={})",
            bytes.len()
        ));
    }
    Ok(&bytes[off..end])
}

#[inline]
pub fn read_u32_le_at(bytes: &[u8], pos: &mut usize, field: &'static str) -> Result<u32, String> {
    let s = take(bytes, pos, 4, field)?;
    Ok(u32::from_le_bytes(s.try_into().unwrap()))
}

#[inline]
pub fn read_u64_le_at(bytes: &[u8], pos: &mut usize, field: &'static str) -> Result<u64, String> {
    let s = take(bytes, pos, 8, field)?;
    Ok(u64::from_le_bytes(s.try_into().unwrap()))
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

#[inline]
fn bytes_to_f32_vec(raw: &[u8]) -> Vec<f32> {
    debug_assert!(raw.len() % 4 == 0);
    let n = raw.len() / 4;

    let mut out = Vec::<f32>::with_capacity(n);
    unsafe {
        out.set_len(n);
        std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
    }
    out
}

#[inline]
fn bytes_to_f64_vec(raw: &[u8]) -> Vec<f64> {
    debug_assert!(raw.len() % 8 == 0);
    let n = raw.len() / 8;

    let mut out = Vec::<f64>::with_capacity(n);
    unsafe {
        out.set_len(n);
        std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
    }
    out
}

#[inline]
fn bda_has_array_kind(bda: &BinaryDataArray, tail: u32) -> bool {
    bda.cv_params.iter().any(|p| {
        p.accession
            .as_deref()
            .map(|a| parse_accession_tail_str(a) == tail)
            .unwrap_or(false)
    })
}

#[inline]
fn ensure_numeric_flag(bda: &mut BinaryDataArray, nt: NumericType) {
    let want = match nt {
        NumericType::Float16 => ACC_16BIT_FLOAT,
        NumericType::Float32 => ACC_32BIT_FLOAT,
        NumericType::Float64 => ACC_64BIT_FLOAT,
        NumericType::Int32 => ACC_32BIT_INTEGER,
        NumericType::Int64 => ACC_64BIT_INTEGER,
        NumericType::Int16 => ACC_16BIT_INTEGER,
    };

    let mut has_want = false;

    let mut i = 0usize;
    while i < bda.cv_params.len() {
        let tail = parse_accession_tail(bda.cv_params[i].accession.as_deref());

        if tail == want {
            has_want = true;
            i += 1;
            continue;
        }

        if tail == ACC_16BIT_FLOAT
            || tail == ACC_32BIT_FLOAT
            || tail == ACC_64BIT_FLOAT
            || tail == ACC_16BIT_INTEGER
            || tail == ACC_32BIT_INTEGER
            || tail == ACC_64BIT_INTEGER
        {
            bda.cv_params.remove(i);
            continue;
        }

        i += 1;
    }

    if !has_want {
        bda.cv_params.push(ms_numeric_param(want));
    }

    bda.numeric_type = Some(nt);
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
    pub parent_index: u32,
    pub tag_id: TagId,
    pub accession: Option<String>,
    pub unit_accession: Option<String>,
    pub value: MetadatumValue,
}

#[inline]
fn ms_numeric_param(accession_tail: u32) -> CvParam {
    let name = match accession_tail {
        ACC_16BIT_FLOAT => "16-bit float",
        ACC_32BIT_FLOAT => "32-bit float",
        ACC_64BIT_FLOAT => "64-bit float",
        ACC_32BIT_INTEGER => "32-bit integer",
        ACC_64BIT_INTEGER => "64-bit integer",
        _ => "numeric",
    };

    CvParam {
        cv_ref: Some("MS".to_string()),
        accession: Some(format!("MS:{:07}", accession_tail)),
        name: name.to_string(),
        value: None,
        unit_cv_ref: None,
        unit_name: None,
        unit_accession: None,
    }
}

fn parse_global_metadata_section(bytes: &[u8], header: &Header) -> Result<Vec<Metadatum>, String> {
    let start = usize::try_from(header.off_global_meta)
        .map_err(|_| "global metadata offsets overflow".to_string())?;
    let len = usize::try_from(header.len_global_meta)
        .map_err(|_| "global metadata length overflow".to_string())?;

    let end = start
        .checked_add(len)
        .ok_or_else(|| "global metadata end overflow".to_string())?;

    if start >= end {
        return Err("invalid global metadata offsets: start >= end".to_string());
    }

    let slice = bytes
        .get(start..end)
        .ok_or_else(|| "invalid global metadata offsets: end out of bounds".to_string())?;

    parse_global_metadata(
        slice,
        0,
        header.global_meta_count,
        header.global_meta_num_count,
        header.global_meta_str_count,
        header.compression_codec,
        header.global_meta_uncompressed_bytes,
    )
}

#[inline]
fn parse_source_file_ref_list(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    metadata: &[&Metadatum],
    run_id: u32,
) -> Option<SourceFileRefList> {
    let mut list_ids =
        unique_ids(children_lookup.get_children_with_tag(run_id, TagId::SourceFileRefList));

    if list_ids.is_empty() {
        list_ids = ChildrenLookup::all_ids(metadata, TagId::SourceFileRefList);

        if run_id != 0 && list_ids.len() > 1 {
            let filtered: Vec<u32> = list_ids
                .iter()
                .copied()
                .filter(|&id| is_child_of(owner_rows, id, run_id))
                .collect();
            if !filtered.is_empty() {
                list_ids = filtered;
            }
        }
    }

    let list_id = list_ids.first().copied()?;
    let list_rows = ChildrenLookup::rows_for_owner(owner_rows, list_id);

    let mut count = b000_attr_text(list_rows, ACC_ATTR_COUNT).and_then(|s| s.parse::<usize>().ok());

    let mut ref_ids =
        unique_ids(children_lookup.get_children_with_tag(list_id, TagId::SourceFileRef));

    if ref_ids.is_empty() {
        ref_ids = ChildrenLookup::all_ids(metadata, TagId::SourceFileRef);

        if ref_ids.len() > 1 {
            let filtered: Vec<u32> = ref_ids
                .iter()
                .copied()
                .filter(|&id| is_child_of(owner_rows, id, list_id))
                .collect();
            if !filtered.is_empty() {
                ref_ids = filtered;
            }
        }
    }

    let mut source_file_refs = Vec::with_capacity(ref_ids.len());
    for rid in ref_ids {
        let rows = ChildrenLookup::rows_for_owner(owner_rows, rid);
        if let Some(r) = b000_attr_text(rows, ACC_ATTR_REF) {
            if !r.is_empty() {
                source_file_refs.push(SourceFileRef { r#ref: r });
            }
        }
    }

    if source_file_refs.is_empty() {
        return None;
    }

    if count.is_none() {
        count = Some(source_file_refs.len());
    }

    Some(SourceFileRefList {
        count,
        source_file_refs,
    })
}

#[inline]
fn parse_binaries(
    bytes: &[u8],
    header: &Header,
) -> Result<
    (
        Vec<Option<Vec<(u32, ArrayData)>>>,
        Vec<Option<Vec<(u32, ArrayData)>>>,
    ),
    String,
> {
    #[inline]
    fn dtype_to_layout(dtype: u8, field: &'static str) -> Result<(usize, NumericType), String> {
        match dtype {
            1 => Ok((8, NumericType::Float64)),
            2 => Ok((4, NumericType::Float32)),
            3 => Ok((2, NumericType::Float16)),
            4 => Ok((2, NumericType::Int16)),
            5 => Ok((4, NumericType::Int32)),
            6 => Ok((8, NumericType::Int64)),
            _ => Err(format!(
                "{field}: invalid dtype {dtype} (expected 1=f64, 2=f32, 3=f16, 4=i16, 5=i32, 6=i64)"
            )),
        }
    }

    #[derive(Clone, Copy)]
    struct A0Entry {
        a1_start: u64,
        a1_count: u64,
    }

    #[derive(Clone, Copy)]
    struct A1Ref {
        array_type: u32,
        dtype: u8,
        block_id: u32,
        element_off: u64,
        len_elements: u64,
    }

    #[inline]
    fn parse_a0_section(
        bytes: &[u8],
        off: u64,
        len: u64,
        item_count: u32,
        field: &'static str,
    ) -> Result<Vec<A0Entry>, String> {
        let expected = (item_count as u64)
            .checked_mul(A0_ENTRY_SIZE as u64)
            .ok_or_else(|| format!("{field}: size overflow"))?;

        if len != expected {
            return Err(format!(
                "{field}: unexpected byte length (got={len}, expected={expected})"
            ));
        }

        let raw = slice_at(bytes, off, len, field)?;
        let mut pos = 0usize;

        let mut out = Vec::with_capacity(item_count as usize);
        for _ in 0..item_count {
            let a1_start = read_u64_le_at(raw, &mut pos, "arr_ref_start")?;
            let a1_count = read_u64_le_at(raw, &mut pos, "arr_ref_count")?;
            out.push(A0Entry { a1_start, a1_count });
        }

        Ok(out)
    }

    #[inline]
    fn parse_a1_section(
        bytes: &[u8],
        off: u64,
        len: u64,
        field: &'static str,
    ) -> Result<Vec<A1Ref>, String> {
        let entry = A1_ENTRY_SIZE;

        if len % entry != 0 {
            return Err(format!("{field}: len not multiple of {entry}"));
        }

        let raw = slice_at(bytes, off, len, field)?;
        let mut pos = 0usize;

        let count = usize::try_from(len / entry).map_err(|_| format!("{field}: count overflow"))?;
        let mut out = Vec::with_capacity(count);

        for _ in 0..count {
            let element_off = read_u64_le_at(raw, &mut pos, "off_element")?;
            let len_elements = read_u64_le_at(raw, &mut pos, "len_element")?;
            let block_id = read_u32_le_at(raw, &mut pos, "block_id")?;
            let array_type = read_u32_le_at(raw, &mut pos, "array_type")?;
            let dtype = take(raw, &mut pos, 1, "dtype")?[0];
            let _ = take(raw, &mut pos, 7, "reserved")?;

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

    #[inline]
    fn bytes_to_u16_vec(raw: &[u8]) -> Vec<u16> {
        debug_assert!(raw.len() % 2 == 0);
        let n = raw.len() / 2;
        let mut out = Vec::<u16>::with_capacity(n);
        unsafe {
            out.set_len(n);
            std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
        }
        out
    }

    #[inline]
    fn bytes_to_i32_vec(raw: &[u8]) -> Vec<i32> {
        debug_assert!(raw.len() % 4 == 0);
        let n = raw.len() / 4;
        let mut out = Vec::<i32>::with_capacity(n);
        unsafe {
            out.set_len(n);
            std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
        }
        out
    }

    #[inline]
    fn bytes_to_i16_vec(raw: &[u8]) -> Vec<i16> {
        debug_assert!(raw.len() % 2 == 0);
        let n = raw.len() / 2;

        let mut out = Vec::<i16>::with_capacity(n);
        unsafe {
            out.set_len(n);
            std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
        }
        out
    }

    #[inline]
    fn bytes_to_i64_vec(raw: &[u8]) -> Vec<i64> {
        debug_assert!(raw.len() % 8 == 0);
        let n = raw.len() / 8;
        let mut out = Vec::<i64>::with_capacity(n);
        unsafe {
            out.set_len(n);
            std::ptr::copy_nonoverlapping(raw.as_ptr(), out.as_mut_ptr() as *mut u8, raw.len());
        }
        out
    }

    let spec_entries = parse_a0_section(
        bytes,
        header.off_spec_entries,
        header.len_spec_entries,
        header.spectrum_count,
        "spec_entries(A0)",
    )?;

    let chrom_entries = parse_a0_section(
        bytes,
        header.off_chrom_entries,
        header.len_chrom_entries,
        header.chrom_count,
        "chrom_entries(B0)",
    )?;

    let spec_a1 = parse_a1_section(
        bytes,
        header.off_spec_arrayrefs,
        header.len_spec_arrayrefs,
        "spec_arrayrefs(A1)",
    )?;

    let chrom_a1 = parse_a1_section(
        bytes,
        header.off_chrom_arrayrefs,
        header.len_chrom_arrayrefs,
        "chrom_arrayrefs(B1)",
    )?;

    let container_spect = slice_at(
        bytes,
        header.off_container_spect,
        header.len_container_spect,
        "container_spect",
    )?;

    let container_chrom = slice_at(
        bytes,
        header.off_container_chrom,
        header.len_container_chrom,
        "container_chrom",
    )?;

    let mut spect_view = ContainerView::new(
        container_spect,
        header.block_count_spect,
        header.compression_level,
        header.array_filter,
        "container_spect",
    )?;

    let mut chrom_view = ContainerView::new(
        container_chrom,
        header.block_count_chrom,
        header.compression_level,
        header.array_filter,
        "container_chrom",
    )?;

    let spec_count = header.spectrum_count as usize;
    let mut spectra_arrays: Vec<Option<Vec<(u32, ArrayData)>>> = vec![None; spec_count];

    for i in 0..spec_count {
        let e = spec_entries[i];

        let start = usize::try_from(e.a1_start).map_err(|_| "A0: a1_start overflow".to_string())?;
        let count = usize::try_from(e.a1_count).map_err(|_| "A0: a1_count overflow".to_string())?;

        let end = start
            .checked_add(count)
            .ok_or_else(|| "A0: a1 range overflow".to_string())?;

        if end > spec_a1.len() {
            return Err("A0: a1 range out of bounds".to_string());
        }

        let mut arrays: Vec<(u32, ArrayData)> = Vec::with_capacity(count);

        for array_ref in &spec_a1[start..end] {
            let (element_size_bytes, numeric_type) = dtype_to_layout(array_ref.dtype, "A1.dtype")?;

            let raw = spect_view.get_item_from_block(
                array_ref.block_id,
                array_ref.element_off,
                array_ref.len_elements,
                element_size_bytes,
                "container_spect",
            )?;

            let data = match numeric_type {
                NumericType::Float16 => ArrayData::F16(bytes_to_u16_vec(raw)),
                NumericType::Float32 => ArrayData::F32(bytes_to_f32_vec(raw)),
                NumericType::Float64 => ArrayData::F64(bytes_to_f64_vec(raw)),
                NumericType::Int16 => ArrayData::I16(bytes_to_i16_vec(raw)),
                NumericType::Int32 => ArrayData::I32(bytes_to_i32_vec(raw)),
                NumericType::Int64 => ArrayData::I64(bytes_to_i64_vec(raw)),
            };

            arrays.push((array_ref.array_type, data));
        }

        spectra_arrays[i] = Some(arrays);
    }

    let chrom_count = header.chrom_count as usize;
    let mut chrom_arrays: Vec<Option<Vec<(u32, ArrayData)>>> = vec![None; chrom_count];

    for i in 0..chrom_count {
        let e = chrom_entries[i];

        let start = usize::try_from(e.a1_start).map_err(|_| "B0: b1_start overflow".to_string())?;
        let count = usize::try_from(e.a1_count).map_err(|_| "B0: b1_count overflow".to_string())?;

        let end = start
            .checked_add(count)
            .ok_or_else(|| "B0: b1 range overflow".to_string())?;

        if end > chrom_a1.len() {
            return Err("B0: b1 range out of bounds".to_string());
        }

        let mut arrays: Vec<(u32, ArrayData)> = Vec::with_capacity(count);

        for array_ref in &chrom_a1[start..end] {
            let (element_size_bytes, numeric_type) = dtype_to_layout(array_ref.dtype, "B1.dtype")?;

            let raw = chrom_view.get_item_from_block(
                array_ref.block_id,
                array_ref.element_off,
                array_ref.len_elements,
                element_size_bytes,
                "container_chrom",
            )?;

            let data = match numeric_type {
                NumericType::Float16 => ArrayData::F16(bytes_to_u16_vec(raw)),
                NumericType::Float32 => ArrayData::F32(bytes_to_f32_vec(raw)),
                NumericType::Float64 => ArrayData::F64(bytes_to_f64_vec(raw)),
                NumericType::Int16 => ArrayData::I16(bytes_to_i16_vec(raw)),
                NumericType::Int32 => ArrayData::I32(bytes_to_i32_vec(raw)),
                NumericType::Int64 => ArrayData::I64(bytes_to_i64_vec(raw)),
            };

            arrays.push((array_ref.array_type, data));
        }

        chrom_arrays[i] = Some(arrays);
    }

    Ok((spectra_arrays, chrom_arrays))
}

#[inline]
fn attach_pairs_to_run_lists(
    run: &mut Run,
    spectra_arrays: &mut [Option<Vec<(u32, ArrayData)>>],
    chrom_arrays: &mut [Option<Vec<(u32, ArrayData)>>],
) {
    #[inline]
    fn attach_typed_arrays_to_bdal(list: &mut BinaryDataArrayList, arrays: Vec<(u32, ArrayData)>) {
        if list.binary_data_arrays.is_empty() {
            return;
        }

        for (kind, data) in arrays {
            if kind == 0 {
                continue;
            }

            let mut idx = None;
            for (i, bda) in list.binary_data_arrays.iter().enumerate() {
                if bda_has_array_kind(bda, kind) {
                    idx = Some(i);
                    break;
                }
            }

            let Some(i) = idx else { continue };
            let Some(bda) = list.binary_data_arrays.get_mut(i) else {
                continue;
            };

            match data {
                ArrayData::F16(v) => {
                    bda.binary = Some(BinaryData::F16(v));
                    ensure_numeric_flag(bda, NumericType::Float16);
                }
                ArrayData::F32(v) => {
                    bda.binary = Some(BinaryData::F32(v));
                    ensure_numeric_flag(bda, NumericType::Float32);
                }
                ArrayData::F64(v) => {
                    bda.binary = Some(BinaryData::F64(v));
                    ensure_numeric_flag(bda, NumericType::Float64);
                }
                ArrayData::I16(v) => {
                    bda.binary = Some(BinaryData::I16(v));
                    ensure_numeric_flag(bda, NumericType::Int16);
                }
                ArrayData::I32(v) => {
                    bda.binary = Some(BinaryData::I32(v));
                    ensure_numeric_flag(bda, NumericType::Int32);
                }
                ArrayData::I64(v) => {
                    bda.binary = Some(BinaryData::I64(v));
                    ensure_numeric_flag(bda, NumericType::Int64);
                }
            }
        }

        list.count = Some(list.binary_data_arrays.len());
    }

    if let Some(sl) = run.spectrum_list.as_mut() {
        for sp in sl.spectra.iter_mut() {
            let idx = sp.index.unwrap_or(0) as usize;

            let Some(slot) = spectra_arrays.get_mut(idx) else {
                continue;
            };
            let Some(arrays) = slot.take() else { continue };

            if let Some(bdal) = sp.binary_data_array_list.as_mut() {
                attach_typed_arrays_to_bdal(bdal, arrays);
            }
        }
    }

    if let Some(cl) = run.chromatogram_list.as_mut() {
        for ch in cl.chromatograms.iter_mut() {
            let idx = ch.index.unwrap_or(0) as usize;

            let Some(slot) = chrom_arrays.get_mut(idx) else {
                continue;
            };
            let Some(arrays) = slot.take() else { continue };

            if let Some(bdal) = ch.binary_data_array_list.as_mut() {
                attach_typed_arrays_to_bdal(bdal, arrays);
            }
        }
    }
}

#[inline]
fn unique_ids(ids: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(ids.len());
    let mut seen = HashSet::with_capacity(ids.len());
    for &id in ids {
        if seen.insert(id) {
            out.push(id);
        }
    }
    out
}

#[inline]
fn is_child_of(owner_rows: &OwnerRows, child_id: u32, parent_id: u32) -> bool {
    ChildrenLookup::rows_for_owner(owner_rows, child_id)
        .first()
        .map(|m| m.parent_index == parent_id)
        .unwrap_or(false)
}
