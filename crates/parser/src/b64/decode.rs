use std::collections::HashMap;

use crate::{
    b64::utilities::{
        Header, common::*, parse_chromatogram_list, parse_cv_and_user_params, parse_cv_list,
        parse_data_processing_list, parse_file_description::parse_file_description,
        parse_global_metadata::parse_global_metadata, parse_header, parse_instrument_list,
        parse_metadata, parse_referenceable_param_group_list, parse_sample_list,
        parse_scan_settings_list, parse_software_list, parse_spectrum_list,
    },
    mzml::{attr_meta::*, schema::TagId, structs::*},
};

pub const INDEX_ENTRY_SIZE: usize = 32;
const BLOCK_DIR_ENTRY_SIZE: usize = 32;

const ARRAY_FILTER_BYTE_SHUFFLE: u8 = 1;

const ACC_32BIT_FLOAT: u32 = 1_000_521;
const ACC_64BIT_FLOAT: u32 = 1_000_523;
const ACC_16BIT_FLOAT: u32 = 1_000_520;
const ACC_16BIT_INTEGER: u32 = 1_000_518;
const ACC_32BIT_INTEGER: u32 = 1_000_519;
const ACC_64BIT_INTEGER: u32 = 1_000_522;

pub fn decode(bytes: &[u8]) -> Result<MzML, String> {
    let header = parse_header(bytes)?;
    let global_meta = parse_global_metadata_section(bytes, &header)?;
    let global_child_index = ChildIndex::new(&global_meta);

    let global_meta_ref: Vec<&Metadatum> = global_meta.iter().collect();

    let cv_list = parse_cv_list(&global_meta_ref, &global_child_index);

    Ok(MzML {
        cv_list,
        file_description: parse_file_description(&global_meta_ref, &global_child_index)
            .expect("missing <fileDescription> in global metadata"),
        referenceable_param_group_list: parse_referenceable_param_group_list(
            &global_meta_ref,
            &global_child_index,
        ),
        sample_list: parse_sample_list(&global_meta_ref, &global_child_index),
        instrument_list: parse_instrument_list(&global_meta_ref, &global_child_index),
        software_list: parse_software_list(&global_meta_ref, &global_child_index),
        data_processing_list: parse_data_processing_list(&global_meta_ref, &global_child_index),
        scan_settings_list: parse_scan_settings_list(&global_meta_ref, &global_child_index),
        run: parse_run(bytes, &header, &global_meta)?,
    })
}

#[inline]
fn parse_run(bytes: &[u8], header: &Header, global_meta: &[Metadatum]) -> Result<Run, String> {
    let spec_meta = parse_metadata_section(
        bytes,
        header,
        header.off_spec_meta,
        header.off_chrom_meta,
        header.spectrum_count,
        header.spec_meta_count,
        header.spec_num_count,
        header.spec_str_count,
        header.size_spec_meta_uncompressed,
    );

    let chrom_meta = parse_metadata_section(
        bytes,
        header,
        header.off_chrom_meta,
        header.off_global_meta,
        header.chrom_count,
        header.chrom_meta_count,
        header.chrom_num_count,
        header.chrom_str_count,
        header.size_chrom_meta_uncompressed,
    );

    let run_child_index = ChildIndex::new(global_meta);

    let mut owner_rows: HashMap<u32, Vec<&Metadatum>> =
        HashMap::with_capacity(global_meta.len() / 2 + 1);
    for m in global_meta {
        owner_rows.entry(m.owner_id).or_default().push(m);
    }

    let run_id = global_meta
        .iter()
        .find(|m| m.tag_id == TagId::Run)
        .map(|m| m.owner_id)
        .unwrap_or(0);

    let run_rows = rows_for_owner(&owner_rows, run_id);

    let id = b000_attr_text(run_rows, ACC_ATTR_ID).unwrap_or_default();
    let start_time_stamp =
        b000_attr_text(run_rows, ACC_ATTR_START_TIME_STAMP).filter(|s| !s.is_empty());

    let default_instrument_configuration_ref =
        b000_attr_text(run_rows, ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF)
            .or_else(|| b000_attr_text(run_rows, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF))
            .filter(|s| !s.is_empty());

    let sample_ref = b000_attr_text(run_rows, ACC_ATTR_SAMPLE_REF).filter(|s| !s.is_empty());

    let mut params_meta = Vec::with_capacity(
        run_rows.len() + child_params_for_parent(&owner_rows, &run_child_index, run_id).len(),
    );
    params_meta.extend(run_rows.iter().copied());
    params_meta.extend(child_params_for_parent(
        &owner_rows,
        &run_child_index,
        run_id,
    ));

    let (cv_params, user_params) = parse_cv_and_user_params(&params_meta);

    let global_meta_ref: Vec<&Metadatum> = global_meta.iter().collect();

    let source_file_ref_list =
        parse_source_file_ref_list(&owner_rows, &run_child_index, &global_meta_ref, run_id);

    let spec_child_index = ChildIndex::new(&spec_meta);
    let chrom_child_index = ChildIndex::new(&chrom_meta);

    let spec_meta_ref: Vec<&Metadatum> = spec_meta.iter().collect();
    let chrom_meta_ref: Vec<&Metadatum> = chrom_meta.iter().collect();

    let spectrum_list = parse_spectrum_list(&spec_meta_ref, &spec_child_index);
    let chromatogram_list = parse_chromatogram_list(&chrom_meta_ref, &chrom_child_index);

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

#[inline(always)]
fn byte_unshuffle_into(input: &[u8], output: &mut [u8], elem_size: usize) {
    assert_eq!(input.len(), output.len(), "in/out size mismatch");
    assert_eq!(input.len() % elem_size, 0, "len not multiple of elem_size");

    match elem_size {
        4 => unshuffle4(input, output),
        8 => unshuffle8(input, output),
        _ => unshuffle_generic(input, output, elem_size),
    }
}

#[inline(always)]
fn unshuffle4(input: &[u8], output: &mut [u8]) {
    let n = input.len() / 4;

    let (b0, rest) = input.split_at(n);
    let (b1, rest) = rest.split_at(n);
    let (b2, b3) = rest.split_at(n);

    for i in 0..n {
        let o = i * 4;
        output[o] = b0[i];
        output[o + 1] = b1[i];
        output[o + 2] = b2[i];
        output[o + 3] = b3[i];
    }
}

#[inline(always)]
fn unshuffle8(input: &[u8], output: &mut [u8]) {
    let n = input.len() / 8;

    let (b0, rest) = input.split_at(n);
    let (b1, rest) = rest.split_at(n);
    let (b2, rest) = rest.split_at(n);
    let (b3, rest) = rest.split_at(n);
    let (b4, rest) = rest.split_at(n);
    let (b5, rest) = rest.split_at(n);
    let (b6, b7) = rest.split_at(n);

    for i in 0..n {
        let o = i * 8;
        output[o] = b0[i];
        output[o + 1] = b1[i];
        output[o + 2] = b2[i];
        output[o + 3] = b3[i];
        output[o + 4] = b4[i];
        output[o + 5] = b5[i];
        output[o + 6] = b6[i];
        output[o + 7] = b7[i];
    }
}

#[inline(always)]
fn unshuffle_generic(input: &[u8], output: &mut [u8], elem_size: usize) {
    let count = input.len() / elem_size;
    for b in 0..elem_size {
        let in_base = b * count;
        for e in 0..count {
            output[b + e * elem_size] = input[in_base + e];
        }
    }
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
    pub owner_id: u32,
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

fn parse_metadata_section(
    bytes: &[u8],
    header: &Header,
    start_off: u64,
    end_off: u64,
    item_count: u32,
    meta_count: u32,
    num_count: u32,
    str_count: u32,
    expected_uncompressed: u64,
) -> Vec<Metadatum> {
    let c0 = start_off as usize;
    let c1 = end_off as usize;

    assert!(c0 < c1, "invalid metadata offsets: start >= end");
    assert!(
        c1 <= bytes.len(),
        "invalid metadata offsets: end out of bounds"
    );

    let slice = &bytes[c0..c1];

    let expected = usize::try_from(expected_uncompressed).expect("metadata expected size overflow");

    parse_metadata(
        slice,
        item_count,
        meta_count,
        num_count,
        str_count,
        header.codec_id,
        expected,
    )
    .expect("parse_metadata failed")
}

fn parse_global_metadata_section(bytes: &[u8], header: &Header) -> Result<Vec<Metadatum>, String> {
    let start = header.off_global_meta as usize;
    let len = header.len_global_meta as usize;
    let end = start
        .checked_add(len)
        .ok_or_else(|| "global metadata end overflow".to_string())?;

    if start >= end {
        return Err("invalid global metadata offsets: start >= end".to_string());
    }
    if end > bytes.len() {
        return Err("invalid global metadata offsets: end out of bounds".to_string());
    }

    let slice = &bytes[start..end];

    parse_global_metadata(
        slice,
        0,
        header.global_meta_count,
        header.global_num_count,
        header.global_str_count,
        header.codec_id,
        header.size_global_meta_uncompressed,
    )
}

#[inline]
fn parse_source_file_ref_list(
    owner_rows: &HashMap<u32, Vec<&Metadatum>>,
    child_index: &ChildIndex,
    metadata: &[&Metadatum],
    run_id: u32,
) -> Option<SourceFileRefList> {
    let mut list_ids = unique_ids(child_index.ids(run_id, TagId::SourceFileRefList));

    if list_ids.is_empty() {
        list_ids = ordered_unique_owner_ids(metadata, TagId::SourceFileRefList);

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
    let list_rows = rows_for_owner(owner_rows, list_id);

    let mut count = b000_attr_text(list_rows, ACC_ATTR_COUNT).and_then(|s| s.parse::<usize>().ok());

    let mut ref_ids = unique_ids(child_index.ids(list_id, TagId::SourceFileRef));

    if ref_ids.is_empty() {
        ref_ids = ordered_unique_owner_ids(metadata, TagId::SourceFileRef);

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
        let rows = rows_for_owner(owner_rows, rid);
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
            1 => Ok((4, NumericType::Float32)),
            2 => Ok((8, NumericType::Float64)),
            3 => Ok((2, NumericType::Float16)),
            4 => Ok((2, NumericType::Int16)),
            5 => Ok((4, NumericType::Int32)),
            6 => Ok((8, NumericType::Int64)),
            _ => Err(format!(
                "{field}: invalid dtype {dtype} (expected 1=f32, 2=f64, 3=f16, 4=i16, 5=i32, 6=i64)"
            )),
        }
    }

    #[derive(Clone, Copy)]
    struct A0Entry {
        a1_start: u32,
        a1_count: u32,
    }

    #[derive(Clone, Copy)]
    struct A1Ref {
        array_type: u32,
        block_id: u32,
        element_off: u64,
        len: u32,
        dtype: u8,
    }

    #[derive(Clone, Copy)]
    struct BlockDirEntry {
        comp_off: u64,
        comp_size: u64,
        uncomp_bytes: u64,
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
            .checked_mul(INDEX_ENTRY_SIZE as u64)
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
            let a1_start = read_u32_le_at(raw, &mut pos, "a1_start")?;
            let a1_count = read_u32_le_at(raw, &mut pos, "a1_count")?;
            let _default_len = read_u32_le_at(raw, &mut pos, "default_array_len")?;
            let _ = take(raw, &mut pos, 20, "reserved")?;
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
        if len % (INDEX_ENTRY_SIZE as u64) != 0 {
            return Err(format!("{field}: len not multiple of {INDEX_ENTRY_SIZE}"));
        }
        let raw = slice_at(bytes, off, len, field)?;
        let mut pos = 0usize;

        let count = (len / (INDEX_ENTRY_SIZE as u64)) as usize;
        let mut out = Vec::with_capacity(count);

        for _ in 0..count {
            let array_type = read_u32_le_at(raw, &mut pos, "array_type")?;
            let block_id = read_u32_le_at(raw, &mut pos, "block_id")?;
            let element_off = read_u64_le_at(raw, &mut pos, "element_off")?;
            let len = read_u32_le_at(raw, &mut pos, "len")?;
            let dtype = take(raw, &mut pos, 1, "dtype")?[0];
            let _ = take(raw, &mut pos, 11, "reserved")?;
            out.push(A1Ref {
                array_type,
                block_id,
                element_off,
                len,
                dtype,
            });
        }
        Ok(out)
    }

    #[inline]
    fn parse_container_dir(
        container: &[u8],
        block_count: u32,
        field: &'static str,
    ) -> Result<(Vec<BlockDirEntry>, usize), String> {
        let bc = block_count as usize;
        let dir_bytes = bc
            .checked_mul(BLOCK_DIR_ENTRY_SIZE)
            .ok_or_else(|| format!("{field}: dir size overflow"))?;

        if container.len() < dir_bytes {
            return Err(format!("{field}: too small for directory"));
        }

        let dir_raw = &container[..dir_bytes];
        let mut pos = 0usize;

        let mut dir = Vec::with_capacity(bc);
        for _ in 0..bc {
            let comp_off = read_u64_le_at(dir_raw, &mut pos, "comp_off")?;
            let comp_size = read_u64_le_at(dir_raw, &mut pos, "comp_size")?;
            let uncomp_bytes = read_u64_le_at(dir_raw, &mut pos, "uncomp_bytes")?;
            let _ = take(dir_raw, &mut pos, 8, "reserved")?;
            dir.push(BlockDirEntry {
                comp_off,
                comp_size,
                uncomp_bytes,
            });
        }

        Ok((dir, dir_bytes))
    }

    #[inline]
    fn ensure_block(
        container: &[u8],
        comp_buf_start: usize,
        dir: &[BlockDirEntry],
        cache: &mut [Option<Vec<u8>>],
        scratch: &mut Vec<u8>,
        block_elem_sizes: &mut [usize],
        block_id: u32,
        elem_size: usize,
        compression_level: u8,
        array_filter: u8,
        field: &'static str,
    ) -> Result<(), String> {
        let i = block_id as usize;
        if i >= cache.len() {
            return Err(format!("{field}: block_id out of range: {block_id}"));
        }

        if array_filter == ARRAY_FILTER_BYTE_SHUFFLE && elem_size > 1 {
            let prev = block_elem_sizes[i];
            if prev == 0 {
                block_elem_sizes[i] = elem_size;
            } else if prev != elem_size {
                return Err(format!(
                    "{field}: block elem_size mismatch for block_id={block_id} (prev={prev}, now={elem_size})"
                ));
            }
        }

        if cache[i].is_some() {
            return Ok(());
        }

        let e = dir[i];

        let comp_off =
            usize::try_from(e.comp_off).map_err(|_| format!("{field}: comp_off overflow"))?;
        let comp_size =
            usize::try_from(e.comp_size).map_err(|_| format!("{field}: comp_size overflow"))?;
        let expected = usize::try_from(e.uncomp_bytes)
            .map_err(|_| format!("{field}: uncomp_bytes overflow"))?;

        let start = comp_buf_start
            .checked_add(comp_off)
            .ok_or_else(|| format!("{field}: comp start overflow"))?;
        let end = start
            .checked_add(comp_size)
            .ok_or_else(|| format!("{field}: comp end overflow"))?;

        if end > container.len() {
            return Err(format!("{field}: block range out of bounds"));
        }

        let comp = &container[start..end];
        let mut out = if compression_level == 0 {
            comp.to_vec()
        } else {
            decompress_zstd(comp, expected)?
        };

        if out.len() != expected {
            return Err(format!(
                "{field}: bad block size (block_id={block_id}, got={}, expected={})",
                out.len(),
                expected
            ));
        }

        if array_filter == ARRAY_FILTER_BYTE_SHUFFLE && elem_size > 1 {
            scratch.resize(out.len(), 0);
            byte_unshuffle_into(&out, scratch.as_mut_slice(), elem_size);
            std::mem::swap(&mut out, scratch);
        }

        cache[i] = Some(out);
        Ok(())
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

    #[inline]
    fn decode_a1_array(
        container: &[u8],
        comp_buf_start: usize,
        dir: &[BlockDirEntry],
        cache: &mut [Option<Vec<u8>>],
        scratch: &mut Vec<u8>,
        block_elem_sizes: &mut [usize],
        block_id: u32,
        element_off: u64,
        len: u32,
        elem_size: usize,
        nt: NumericType,
        compression_level: u8,
        array_filter: u8,
        field: &'static str,
    ) -> Result<ArrayData, String> {
        ensure_block(
            container,
            comp_buf_start,
            dir,
            cache,
            scratch,
            block_elem_sizes,
            block_id,
            elem_size,
            compression_level,
            array_filter,
            field,
        )?;

        let raw = cache[block_id as usize].as_ref().unwrap().as_slice();

        let off_elems =
            usize::try_from(element_off).map_err(|_| format!("{field}: element_off overflow"))?;
        let len_elems = usize::try_from(len).map_err(|_| format!("{field}: len overflow"))?;

        let off_bytes = off_elems
            .checked_mul(elem_size)
            .ok_or_else(|| format!("{field}: off_bytes overflow"))?;
        let len_bytes = len_elems
            .checked_mul(elem_size)
            .ok_or_else(|| format!("{field}: len_bytes overflow"))?;

        let end = off_bytes
            .checked_add(len_bytes)
            .ok_or_else(|| format!("{field}: slice end overflow"))?;

        if end > raw.len() {
            return Err(format!("{field}: array slice out of bounds"));
        }

        let slice = &raw[off_bytes..end];

        Ok(match nt {
            NumericType::Float16 => ArrayData::F16(bytes_to_u16_vec(slice)),
            NumericType::Float32 => ArrayData::F32(bytes_to_f32_vec(slice)),
            NumericType::Float64 => ArrayData::F64(bytes_to_f64_vec(slice)),
            NumericType::Int16 => ArrayData::I16(bytes_to_i16_vec(slice)),
            NumericType::Int32 => ArrayData::I32(bytes_to_i32_vec(slice)),
            NumericType::Int64 => ArrayData::I64(bytes_to_i64_vec(slice)),
        })
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

    let (dir_spect, comp_start_spect) =
        parse_container_dir(container_spect, header.block_count_spect, "container_spect")?;
    let (dir_chrom, comp_start_chrom) =
        parse_container_dir(container_chrom, header.block_count_chrom, "container_chrom")?;

    let mut cache_spect: Vec<Option<Vec<u8>>> = vec![None; header.block_count_spect as usize];
    let mut cache_chrom: Vec<Option<Vec<u8>>> = vec![None; header.block_count_chrom as usize];

    let mut scratch_spect: Vec<u8> = Vec::new();
    let mut scratch_chrom: Vec<u8> = Vec::new();

    let mut block_elem_spect: Vec<usize> = vec![0; header.block_count_spect as usize];
    let mut block_elem_chrom: Vec<usize> = vec![0; header.block_count_chrom as usize];

    let spec_count = header.spectrum_count as usize;
    let mut spectra_arrays: Vec<Option<Vec<(u32, ArrayData)>>> = vec![None; spec_count];

    for i in 0..spec_count {
        let e = spec_entries[i];

        let start = e.a1_start as usize;
        let count = e.a1_count as usize;
        let end = start
            .checked_add(count)
            .ok_or_else(|| "A0: a1 range overflow".to_string())?;

        if end > spec_a1.len() {
            return Err("A0: a1 range out of bounds".to_string());
        }

        let mut arrays: Vec<(u32, ArrayData)> = Vec::with_capacity(count);

        for r in &spec_a1[start..end] {
            let (elem_size, nt) = dtype_to_layout(r.dtype, "A1.dtype")?;
            let data = decode_a1_array(
                container_spect,
                comp_start_spect,
                &dir_spect,
                &mut cache_spect,
                &mut scratch_spect,
                &mut block_elem_spect,
                r.block_id,
                r.element_off,
                r.len,
                elem_size,
                nt,
                header.compression_level,
                header.array_filter,
                "container_spect",
            )?;
            arrays.push((r.array_type, data));
        }

        spectra_arrays[i] = Some(arrays);
    }

    let chrom_count = header.chrom_count as usize;
    let mut chrom_arrays: Vec<Option<Vec<(u32, ArrayData)>>> = vec![None; chrom_count];

    for i in 0..chrom_count {
        let e = chrom_entries[i];

        let start = e.a1_start as usize;
        let count = e.a1_count as usize;
        let end = start
            .checked_add(count)
            .ok_or_else(|| "B0: b1 range overflow".to_string())?;

        if end > chrom_a1.len() {
            return Err("B0: b1 range out of bounds".to_string());
        }

        let mut arrays: Vec<(u32, ArrayData)> = Vec::with_capacity(count);

        for r in &chrom_a1[start..end] {
            let (elem_size, nt) = dtype_to_layout(r.dtype, "B1.dtype")?;
            let data = decode_a1_array(
                container_chrom,
                comp_start_chrom,
                &dir_chrom,
                &mut cache_chrom,
                &mut scratch_chrom,
                &mut block_elem_chrom,
                r.block_id,
                r.element_off,
                r.len,
                elem_size,
                nt,
                header.compression_level,
                header.array_filter,
                "container_chrom",
            )?;
            arrays.push((r.array_type, data));
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
