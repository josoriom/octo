use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem, slice,
};
use zstd::bulk::compress as zstd_compress;

use crate::{
    BinaryData, NumericType,
    b64::utilities::assign_attributes,
    decode::MetadatumValue,
    mzml::{
        attr_meta::*,
        schema::TagId,
        structs::{
            BinaryDataArray, Chromatogram, CvParam, MzML, Precursor, PrecursorList, Product,
            ProductList, ReferenceableParamGroup, ReferenceableParamGroupRef, ScanList, Spectrum,
        },
    },
};

#[derive(Debug)]
pub struct PackedMeta {
    pub index_offsets: Vec<u32>,          // CI
    pub owner_ids: Vec<u32>,              // MOI
    pub parent_indices: Vec<u32>,         // MPI
    pub tag_ids: Vec<u8>,                 // MTI
    pub ref_codes: Vec<u8>,               // MRI
    pub accession_numbers: Vec<u32>,      // MAN
    pub unit_ref_codes: Vec<u8>,          // MURI
    pub unit_accession_numbers: Vec<u32>, // MUAN
    pub value_kinds: Vec<u8>,             // VK
    pub value_indices: Vec<u32>,          // VI
    pub numeric_values: Vec<f64>,         // VN
    pub string_offsets: Vec<u32>,         // VOFF
    pub string_lengths: Vec<u32>,         // VLEN
    pub string_bytes: Vec<u8>,            // VS
}

#[derive(Debug)]
struct GlobalCounts {
    n_file_description: u32,
    n_run: u32,
    n_ref_param_groups: u32,
    n_samples: u32,
    n_instrument_configs: u32,
    n_software: u32,
    n_data_processing: u32,
    n_acquisition_settings: u32,
    n_cvs: u32,
}

#[derive(Debug)]
struct GlobalMetaItem {
    cvs: Vec<CvParam>,
    tags: Vec<u8>,
    owners: Vec<u32>,
    parents: Vec<u32>,
}

const HEADER_SIZE: usize = 512;
const FILE_TRAILER: [u8; 8] = *b"END\0\0\0\0\0";
const BLOCK_DIR_ENTRY_SIZE: usize = 32;

const TARGET_BLOCK_UNCOMP_BYTES: usize = 64 * 1024 * 1024;

const ACC_MZ_ARRAY: u32 = 1_000_514;
const ACC_INTENSITY_ARRAY: u32 = 1_000_515;
const ACC_TIME_ARRAY: u32 = 1_000_595;

const ACC_32BIT_FLOAT: u32 = 1_000_521;
const ACC_64BIT_FLOAT: u32 = 1_000_523;

const ARRAY_FILTER_NONE: u8 = 0;
const ARRAY_FILTER_BYTE_SHUFFLE: u8 = 1;

#[inline]
fn compress_bytes(input: &[u8], compression_level: u8) -> Vec<u8> {
    if compression_level == 0 {
        return input.to_vec();
    }
    zstd_compress(input, compression_level as i32).expect("zstd compression failed")
}

const DTYPE_F32: u8 = 1;
const DTYPE_F64: u8 = 2;
const DTYPE_F16: u8 = 3;
const DTYPE_I16: u8 = 4;
const DTYPE_I32: u8 = 5;
const DTYPE_I64: u8 = 6;

#[derive(Copy, Clone)]
enum ArrayRef<'a> {
    F16(&'a [u16]),
    F32(&'a [f32]),
    F64(&'a [f64]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
}

impl<'a> ArrayRef<'a> {
    #[inline]
    fn len(self) -> usize {
        match self {
            ArrayRef::F16(s) => s.len(),
            ArrayRef::F32(s) => s.len(),
            ArrayRef::F64(s) => s.len(),
            ArrayRef::I16(s) => s.len(),
            ArrayRef::I32(s) => s.len(),
            ArrayRef::I64(s) => s.len(),
        }
    }
}

#[inline]
fn dtype_elem_size(dtype: u8) -> usize {
    match dtype {
        DTYPE_F16 | DTYPE_I16 => 2,
        DTYPE_F32 | DTYPE_I32 => 4,
        DTYPE_F64 | DTYPE_I64 => 8,
        _ => 1,
    }
}

#[inline]
fn write_u16_slice_le(buf: &mut Vec<u8>, xs: &[u16]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 2);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

#[inline]
fn write_i16_slice_le(buf: &mut Vec<u8>, xs: &[i16]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 2);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

#[inline]
fn write_i32_slice_le(buf: &mut Vec<u8>, xs: &[i32]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 4);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

#[inline]
fn write_i64_slice_le(buf: &mut Vec<u8>, xs: &[i64]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 8);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
}

#[inline]
fn write_array(buf: &mut Vec<u8>, arr: ArrayRef<'_>, dtype: u8) {
    match dtype {
        DTYPE_F16 => match arr {
            ArrayRef::F16(xs) => write_u16_slice_le(buf, xs),
            _ => {}
        },
        DTYPE_F32 => match arr {
            ArrayRef::F32(xs) => write_f32_slice_le(buf, xs),
            ArrayRef::F64(xs) => {
                for &v in xs {
                    write_f64_as_f32(buf, v);
                }
            }
            _ => {}
        },
        DTYPE_F64 => match arr {
            ArrayRef::F64(xs) => write_f64_slice_le(buf, xs),
            ArrayRef::F32(xs) => {
                for &v in xs {
                    write_f64_le(buf, v as f64);
                }
            }
            _ => {}
        },
        DTYPE_I16 => match arr {
            ArrayRef::I16(xs) => write_i16_slice_le(buf, xs),
            _ => {}
        },
        DTYPE_I32 => match arr {
            ArrayRef::I32(xs) => write_i32_slice_le(buf, xs),
            _ => {}
        },
        DTYPE_I64 => match arr {
            ArrayRef::I64(xs) => write_i64_slice_le(buf, xs),
            _ => {}
        },
        _ => {}
    }
}

#[inline]
fn write_u32_slice_le(buf: &mut Vec<u8>, xs: &[u32]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 4);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            write_u32_le(buf, v);
        }
    }
}

#[inline]
fn write_f64_slice_le(buf: &mut Vec<u8>, xs: &[f64]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 8);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            write_f64_le(buf, v);
        }
    }
}

#[inline]
fn write_f32_slice_le(buf: &mut Vec<u8>, xs: &[f32]) {
    if cfg!(target_endian = "little") {
        unsafe {
            let p = xs.as_ptr() as *const u8;
            let b = slice::from_raw_parts(p, xs.len() * 4);
            buf.extend_from_slice(b);
        }
    } else {
        for &v in xs {
            write_f32_le(buf, v);
        }
    }
}

#[inline]
fn byte_shuffle_into(input: &[u8], output: &mut [u8], elem_size: usize) {
    let count = input.len() / elem_size;
    for b in 0..elem_size {
        let out_base = b * count;
        let mut in_i = b;
        for e in 0..count {
            output[out_base + e] = input[in_i];
            in_i += elem_size;
        }
    }
}

#[derive(Clone, Copy)]
struct BlockDirEntry {
    payload_offset: u64,
    payload_size: u64,
    uncompressed_len_bytes: u64,
}

struct BlockBox {
    block_index: Option<u32>,
    buffer: Vec<u8>,
}

struct ContainerBuilder {
    target_block_uncomp_byte_size: usize,
    compression_level: u8,
    do_shuffle: bool,
    boxes: BTreeMap<usize, BlockBox>,
    entries: Vec<BlockDirEntry>,
    compressed: Vec<u8>,
    scratch: Vec<u8>,
}

impl ContainerBuilder {
    #[inline]
    fn new(target_block_uncomp_byte_size: usize, compression_level: u8, do_shuffle: bool) -> Self {
        Self {
            target_block_uncomp_byte_size,
            compression_level,
            do_shuffle,
            boxes: BTreeMap::new(),
            entries: Vec::new(),
            compressed: Vec::new(),
            scratch: Vec::new(),
        }
    }

    #[inline]
    fn add_item_to_box<F>(&mut self, item_bytes: usize, elem_size: usize, write_fn: F) -> (u32, u64)
    where
        F: FnOnce(&mut Vec<u8>),
    {
        let element_size = elem_size.max(1);

        if item_bytes > self.target_block_uncomp_byte_size {
            self.seal_box(element_size);

            let b = self.boxes.entry(element_size).or_insert_with(|| BlockBox {
                block_index: None,
                buffer: Vec::new(),
            });

            let block_id = self.entries.len() as u32;
            self.entries.push(BlockDirEntry {
                payload_offset: 0,
                payload_size: 0,
                uncompressed_len_bytes: 0,
            });
            b.block_index = Some(block_id);

            b.buffer.reserve(item_bytes);
            write_fn(&mut b.buffer);

            self.seal_box(element_size);
            return (block_id, 0);
        }

        self.ensure_box_has_space(item_bytes, element_size);
        let (index, element_off) = {
            let block_box = self.boxes.entry(element_size).or_insert_with(|| BlockBox {
                block_index: None,
                buffer: Vec::new(),
            });

            let box_index = match block_box.block_index {
                Some(idx) => idx,
                None => {
                    let idx = self.entries.len() as u32;
                    self.entries.push(BlockDirEntry {
                        payload_offset: 0,
                        payload_size: 0,
                        uncompressed_len_bytes: 0,
                    });
                    block_box.block_index = Some(idx);
                    idx
                }
            };

            let offset = (block_box.buffer.len() / element_size) as u64;

            block_box.buffer.reserve(item_bytes);
            write_fn(&mut block_box.buffer);

            (box_index, offset)
        };

        (index, element_off)
    }

    #[inline]
    fn seal_box(&mut self, element_size_bytes: usize) {
        let open_box = match self.boxes.get_mut(&element_size_bytes) {
            Some(open_box) => open_box,
            None => return,
        };

        let block_index = match open_box.block_index {
            Some(id) => id,
            None => return,
        };

        if open_box.buffer.is_empty() {
            open_box.block_index = None;
            return;
        }

        let uncompressed_len_bytes = open_box.buffer.len() as u64;
        let payload_offset = self.compressed.len() as u64;

        if self.compression_level == 0 {
            self.entries[block_index as usize] = BlockDirEntry {
                payload_offset,
                payload_size: uncompressed_len_bytes,
                uncompressed_len_bytes: uncompressed_len_bytes,
            };
            self.compressed.extend_from_slice(&open_box.buffer);
            open_box.buffer.clear();
            open_box.block_index = None;
            return;
        }

        let element_size = element_size_bytes.max(1);

        let uncompressed: &[u8] = if self.do_shuffle && element_size > 1 {
            self.scratch.resize(open_box.buffer.len(), 0);
            byte_shuffle_into(
                open_box.buffer.as_slice(),
                self.scratch.as_mut_slice(),
                element_size,
            );
            self.scratch.as_slice()
        } else {
            open_box.buffer.as_slice()
        };

        let compressed = compress_bytes(uncompressed, self.compression_level);
        let compressed_size = compressed.len() as u64;

        self.entries[block_index as usize] = BlockDirEntry {
            payload_offset,
            payload_size: compressed_size,
            uncompressed_len_bytes,
        };

        self.compressed.extend_from_slice(&compressed);
        open_box.buffer.clear();
        open_box.block_index = None;
    }

    #[inline]
    fn ensure_box_has_space(&mut self, item_bytes: usize, element_size_bytes: usize) {
        let element_size_bytes = element_size_bytes.max(1);

        // Check if the current open box for this element size would overflow if we add this item.
        let should_seal_current_box = {
            let block_box = self
                .boxes
                .entry(element_size_bytes)
                .or_insert_with(|| BlockBox {
                    block_index: None,
                    buffer: Vec::new(),
                });

            !block_box.buffer.is_empty()
                && block_box.buffer.len() + item_bytes > self.target_block_uncomp_byte_size
        };

        if should_seal_current_box {
            self.seal_box(element_size_bytes);
        }
    }

    #[inline]
    fn pack(mut self) -> (Vec<u8>, u32) {
        // Ensure all boxes are closed. Flush all open boxes
        loop {
            let mut found = false;
            let mut min_block_index = u32::MAX;
            let mut elem_size_to_flush = 0usize;
            for (elem_size, open_box) in self.boxes.iter() {
                if let Some(block_index) = open_box.block_index {
                    if block_index < min_block_index {
                        min_block_index = block_index;
                        elem_size_to_flush = *elem_size;
                        found = true;
                    }
                }
            }

            if !found {
                break;
            }
            self.seal_box(elem_size_to_flush);
        }

        // Write the container
        let block_count = self.entries.len() as u32;
        let directory_byte_size = self.entries.len() * BLOCK_DIR_ENTRY_SIZE;

        let mut container = Vec::with_capacity(directory_byte_size + self.compressed.len());
        for block in &self.entries {
            write_u64_le(&mut container, block.payload_offset);
            write_u64_le(&mut container, block.payload_size);
            write_u64_le(&mut container, block.uncompressed_len_bytes);
            container.extend_from_slice(&[0u8; 8]);
        }
        container.extend_from_slice(&self.compressed);

        (container, block_count)
    }
}

#[derive(Debug, Default)]
struct NodeIdGen {
    next: u32,
}

impl NodeIdGen {
    #[inline]
    fn new() -> Self {
        Self { next: 1 }
    }

    #[inline]
    fn alloc(&mut self) -> u32 {
        let id = self.next;
        self.next += 1;
        id
    }
}

struct MetaAcc<'a> {
    out: &'a mut Vec<CvParam>,
    tags: &'a mut Vec<u8>,
    owners: &'a mut Vec<u32>,
    parents: &'a mut Vec<u32>,
}

impl<'a> MetaAcc<'a> {
    #[inline]
    fn new(
        out: &'a mut Vec<CvParam>,
        tags: &'a mut Vec<u8>,
        owners: &'a mut Vec<u32>,
        parents: &'a mut Vec<u32>,
    ) -> Self {
        Self {
            out,
            tags,
            owners,
            parents,
        }
    }

    #[inline]
    fn push_tagged_raw(&mut self, tag_id: u8, owner_id: u32, parent_owner_id: u32, cv: CvParam) {
        self.out.push(cv);
        self.tags.push(tag_id);
        self.owners.push(owner_id);
        self.parents.push(parent_owner_id);
    }

    #[inline]
    fn extend_tagged_raw(
        &mut self,
        tag_id: u8,
        owner_id: u32,
        parent_owner_id: u32,
        cvs: &[CvParam],
    ) {
        self.out.extend_from_slice(cvs);
        self.tags.resize(self.tags.len() + cvs.len(), tag_id);
        self.owners.resize(self.owners.len() + cvs.len(), owner_id);
        self.parents
            .resize(self.parents.len() + cvs.len(), parent_owner_id);
    }

    #[inline]
    fn push_tagged_ids(&mut self, tag: TagId, owner_id: u32, parent_owner_id: u32, cv: CvParam) {
        self.push_tagged_raw(tag as u8, owner_id, parent_owner_id, cv);
    }

    #[inline]
    fn extend_tagged_ids(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_owner_id: u32,
        cvs: &[CvParam],
    ) {
        self.extend_tagged_raw(tag as u8, owner_id, parent_owner_id, cvs);
    }

    #[inline]
    fn push_attr_string_tagged_raw(
        &mut self,
        tag_id: u8,
        owner_id: u32,
        parent_owner_id: u32,
        accession_tail: u32,
        value: &str,
    ) {
        if !value.is_empty() {
            self.push_tagged_raw(
                tag_id,
                owner_id,
                parent_owner_id,
                attr_cv_param(accession_tail, value),
            );
        }
    }

    #[inline]
    fn push_attr_u32_tagged_raw(
        &mut self,
        tag_id: u8,
        owner_id: u32,
        parent_owner_id: u32,
        accession_tail: u32,
        value: Option<u32>,
    ) {
        if let Some(v) = value {
            self.push_tagged_raw(
                tag_id,
                owner_id,
                parent_owner_id,
                attr_cv_param(accession_tail, &v.to_string()),
            );
        }
    }

    #[inline]
    fn push_attr_string_tagged_ids(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_owner_id: u32,
        accession_tail: u32,
        value: &str,
    ) {
        self.push_attr_string_tagged_raw(
            tag as u8,
            owner_id,
            parent_owner_id,
            accession_tail,
            value,
        );
    }

    #[inline]
    fn push_attr_usize_tagged_ids(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_owner_id: u32,
        accession_tail: u32,
        value: Option<u32>,
    ) {
        self.push_attr_u32_tagged_raw(tag as u8, owner_id, parent_owner_id, accession_tail, value);
    }

    /// <referenceableParamGroupRef>
    #[inline]
    fn extend_ref_group_cv_params_ids(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_owner_id: u32,
        refs: &[ReferenceableParamGroupRef],
        ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    ) {
        for r in refs {
            if let Some(g) = ref_groups.get(r.r#ref.as_str()) {
                self.extend_tagged_ids(tag, owner_id, parent_owner_id, &g.cv_params);
            }
        }
    }

    #[inline]
    fn push_assigned_attributes_as_cv_params(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_owner_id: u32,
        attrs: Vec<crate::b64::decode::Metadatum>,
    ) {
        for m in attrs {
            let tail = parse_accession_tail(m.accession.as_deref());
            if tail == 0 {
                continue;
            }

            match m.value {
                MetadatumValue::Text(v) => {
                    if v.is_empty() {
                        continue;
                    }
                    self.push_tagged_ids(tag, owner_id, parent_owner_id, attr_cv_param(tail, &v));
                }
                MetadatumValue::Number(n) => {
                    let s = n.to_string();
                    if s.is_empty() {
                        continue;
                    }
                    self.push_tagged_ids(tag, owner_id, parent_owner_id, attr_cv_param(tail, &s));
                }
                MetadatumValue::Empty => {
                    self.push_tagged_ids(tag, owner_id, parent_owner_id, attr_cv_param(tail, ""));
                }
            }
        }
    }

    #[inline]
    fn push_schema_attributes<T: Serialize>(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_owner_id: u32,
        expected: &T,
    ) {
        let attrs = assign_attributes(expected, tag, owner_id, parent_owner_id);
        for m in attrs {
            let tail = parse_accession_tail(m.accession.as_deref());
            if tail == 0 {
                continue;
            }

            let s = match m.value {
                MetadatumValue::Text(v) => v,
                MetadatumValue::Number(n) => n.to_string(),
                MetadatumValue::Empty => continue,
            };

            if s.is_empty() {
                continue;
            }

            self.push_tagged_ids(tag, owner_id, parent_owner_id, attr_cv_param(tail, &s));
        }
    }
}

/// <referenceableParamGroupList>
fn build_ref_group_map<'a>(mzml: &'a MzML) -> HashMap<&'a str, &'a ReferenceableParamGroup> {
    let mut map = HashMap::new();
    if let Some(list) = &mzml.referenceable_param_group_list {
        map = HashMap::with_capacity(list.referenceable_param_groups.len());
        for g in &list.referenceable_param_groups {
            map.insert(g.id.as_str(), g);
        }
    }
    map
}

/// <binaryDataArray>
fn extend_binary_data_array_cv_params_ids(
    meta: &mut MetaAcc<'_>,
    owner_id: u32,
    parent_owner_id: u32,
    ba: &BinaryDataArray,
    x_accession_tail: u32,
    y_accession_tail: u32,
    f32_compress: bool,
) {
    let mut is_x = false;
    let mut is_y = false;

    for cv in &ba.cv_params {
        let tail = parse_accession_tail(cv.accession.as_deref());
        if tail == x_accession_tail {
            is_x = true;
        } else if tail == y_accession_tail {
            is_y = true;
        }
    }

    let desired_float_tail = if f32_compress && (is_x || is_y) {
        Some(ACC_32BIT_FLOAT)
    } else {
        None
    };

    if desired_float_tail.is_none() {
        meta.extend_tagged_ids(
            TagId::BinaryDataArray,
            owner_id,
            parent_owner_id,
            &ba.cv_params,
        );
        return;
    }

    let desired = desired_float_tail.unwrap();
    let mut wrote_float = false;

    for cv in &ba.cv_params {
        let tail = parse_accession_tail(cv.accession.as_deref());

        if tail == ACC_32BIT_FLOAT || tail == ACC_64BIT_FLOAT {
            if !wrote_float {
                meta.push_tagged_ids(
                    TagId::BinaryDataArray,
                    owner_id,
                    parent_owner_id,
                    ms_float_param(desired),
                );
                wrote_float = true;
            }
            continue;
        }

        meta.push_tagged_ids(
            TagId::BinaryDataArray,
            owner_id,
            parent_owner_id,
            cv.clone(),
        );
    }

    if !wrote_float {
        meta.push_tagged_ids(
            TagId::BinaryDataArray,
            owner_id,
            parent_owner_id,
            ms_float_param(desired),
        );
    }
}

#[inline]
fn ms_float_param(accession_tail: u32) -> CvParam {
    let name = if accession_tail == ACC_32BIT_FLOAT {
        "32-bit float"
    } else {
        "64-bit float"
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

/// <scanList>
fn flatten_scan_list_ids(
    meta: &mut MetaAcc<'_>,
    scan_list: &ScanList,
    scan_list_owner_id: u32,
    id_gen: &mut NodeIdGen,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    for scan in &scan_list.scans {
        let scan_id = id_gen.alloc();

        // <scan> attributes
        meta.push_schema_attributes(TagId::Scan, scan_id, scan_list_owner_id, scan);

        // <scan> refGroup
        meta.extend_ref_group_cv_params_ids(
            TagId::Scan,
            scan_id,
            scan_list_owner_id,
            &scan.referenceable_param_group_refs,
            ref_groups,
        );

        // <scan> cvParams
        meta.extend_tagged_ids(TagId::Scan, scan_id, scan_list_owner_id, &scan.cv_params);

        // <scanWindowList>/<scanWindow>
        if let Some(wl) = &scan.scan_window_list {
            for w in &wl.scan_windows {
                let win_id = id_gen.alloc();

                // <scanWindow> attributes
                meta.push_schema_attributes(TagId::ScanWindow, win_id, scan_id, w);

                // <scanWindow> cvParams
                meta.extend_tagged_ids(TagId::ScanWindow, win_id, scan_id, &w.cv_params);
            }
        }
    }
}

/// <precursorList>
fn flatten_precursor_list_ids(
    meta: &mut MetaAcc<'_>,
    precursor_list: &PrecursorList,
    parent_owner_id: u32,
    id_gen: &mut NodeIdGen,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    for p in &precursor_list.precursors {
        flatten_precursor_ids(meta, p, parent_owner_id, id_gen, ref_groups);
    }
}

/// <precursor>
fn flatten_precursor_ids(
    meta: &mut MetaAcc<'_>,
    precursor: &Precursor,
    parent_owner_id: u32,
    id_gen: &mut NodeIdGen,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    let precursor_id = id_gen.alloc();

    let base = meta.out.len();

    meta.push_schema_attributes(TagId::Precursor, precursor_id, parent_owner_id, precursor);

    if let Some(r) = precursor.spectrum_ref.as_deref() {
        let mut already = false;

        for i in base..meta.out.len() {
            if meta.owners[i] != precursor_id {
                continue;
            }
            if meta.tags[i] != TagId::Precursor as u8 {
                continue;
            }

            let tail = parse_accession_tail(meta.out[i].accession.as_deref());
            if tail == ACC_ATTR_SPECTRUM_REF {
                already = true;
                break;
            }
        }

        if !already {
            meta.push_attr_string_tagged_ids(
                TagId::Precursor,
                precursor_id,
                parent_owner_id,
                ACC_ATTR_SPECTRUM_REF,
                r,
            );
        }
    }

    if let Some(iw) = &precursor.isolation_window {
        let iw_id = id_gen.alloc();

        meta.push_schema_attributes(TagId::IsolationWindow, iw_id, precursor_id, iw);

        meta.extend_ref_group_cv_params_ids(
            TagId::IsolationWindow,
            iw_id,
            precursor_id,
            &iw.referenceable_param_group_refs,
            ref_groups,
        );

        meta.extend_tagged_ids(TagId::IsolationWindow, iw_id, precursor_id, &iw.cv_params);
    }

    if let Some(sil) = &precursor.selected_ion_list {
        let sil_id = id_gen.alloc();
        meta.push_schema_attributes(TagId::SelectedIonList, sil_id, precursor_id, sil);
        for ion in &sil.selected_ions {
            let ion_id = id_gen.alloc();

            meta.push_schema_attributes(TagId::SelectedIon, ion_id, sil_id, ion);

            meta.extend_ref_group_cv_params_ids(
                TagId::SelectedIon,
                ion_id,
                precursor_id,
                &ion.referenceable_param_group_refs,
                ref_groups,
            );

            meta.extend_tagged_ids(TagId::SelectedIon, ion_id, precursor_id, &ion.cv_params);
        }
    }

    if let Some(act) = &precursor.activation {
        let act_id = id_gen.alloc();

        meta.push_schema_attributes(TagId::Activation, act_id, precursor_id, act);

        meta.extend_ref_group_cv_params_ids(
            TagId::Activation,
            act_id,
            precursor_id,
            &act.referenceable_param_group_refs,
            ref_groups,
        );

        meta.extend_tagged_ids(TagId::Activation, act_id, precursor_id, &act.cv_params);
    }
}

/// <productList>
fn flatten_product_list_ids(
    meta: &mut MetaAcc<'_>,
    product_list: &ProductList,
    parent_owner_id: u32,
    id_gen: &mut NodeIdGen,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    for p in &product_list.products {
        flatten_product_ids(meta, p, parent_owner_id, id_gen, ref_groups);
    }
}

/// <product>
fn flatten_product_ids(
    meta: &mut MetaAcc<'_>,
    product: &Product,
    parent_owner_id: u32,
    id_gen: &mut NodeIdGen,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    let product_id = id_gen.alloc();
    meta.push_schema_attributes(TagId::Product, product_id, parent_owner_id, product);

    if let Some(iw) = &product.isolation_window {
        let iw_id = id_gen.alloc();

        meta.push_schema_attributes(TagId::IsolationWindow, iw_id, product_id, iw);

        // Tag: IsolationWindow
        meta.extend_ref_group_cv_params_ids(
            TagId::IsolationWindow,
            iw_id,
            product_id,
            &iw.referenceable_param_group_refs,
            ref_groups,
        );

        // Tag: IsolationWindow
        meta.extend_tagged_ids(TagId::IsolationWindow, iw_id, product_id, &iw.cv_params);
    }
}

/// <spectrum>
fn flatten_spectrum_metadata_into_owned(
    meta: &mut MetaAcc<'_>,
    spectrum: &Spectrum,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    spectrum_id: u32,
    parent_owner_id: u32,
    id_gen: &mut NodeIdGen,
    x_accession_tail: u32,
    y_accession_tail: u32,
    x_store_f64: bool,
    y_store_f64: bool,
    f32_compress: bool,
) {
    let attrs = assign_attributes(spectrum, TagId::Spectrum, spectrum_id, parent_owner_id);
    meta.push_assigned_attributes_as_cv_params(
        TagId::Spectrum,
        spectrum_id,
        parent_owner_id,
        attrs,
    );

    meta.extend_ref_group_cv_params_ids(
        TagId::Spectrum,
        spectrum_id,
        parent_owner_id,
        &spectrum.referenceable_param_group_refs,
        ref_groups,
    );

    meta.extend_tagged_ids(
        TagId::Spectrum,
        spectrum_id,
        parent_owner_id,
        &spectrum.cv_params,
    );

    if let Some(sd) = &spectrum.spectrum_description {
        let sd_id = id_gen.alloc();
        meta.extend_ref_group_cv_params_ids(
            TagId::SpectrumDescription,
            sd_id,
            spectrum_id,
            &sd.referenceable_param_group_refs,
            ref_groups,
        );

        meta.extend_tagged_ids(
            TagId::SpectrumDescription,
            sd_id,
            spectrum_id,
            &sd.cv_params,
        );

        if let Some(sl) = &sd.scan_list {
            let sl_id = id_gen.alloc();

            let attrs = assign_attributes(sl, TagId::ScanList, sl_id, sd_id);
            meta.push_assigned_attributes_as_cv_params(TagId::ScanList, sl_id, sd_id, attrs);

            meta.extend_tagged_ids(TagId::ScanList, sl_id, sd_id, &sl.cv_params);

            flatten_scan_list_ids(meta, sl, sl_id, id_gen, ref_groups);
        }

        if let Some(pl) = &sd.precursor_list {
            let pl_id = id_gen.alloc();

            let attrs = assign_attributes(pl, TagId::PrecursorList, pl_id, sd_id);
            meta.push_assigned_attributes_as_cv_params(TagId::PrecursorList, pl_id, sd_id, attrs);

            flatten_precursor_list_ids(meta, pl, pl_id, id_gen, ref_groups);
        }

        if let Some(pl) = &sd.product_list {
            let pl_id = id_gen.alloc();

            let attrs = assign_attributes(pl, TagId::ProductList, pl_id, sd_id);
            meta.push_assigned_attributes_as_cv_params(TagId::ProductList, pl_id, sd_id, attrs);

            flatten_product_list_ids(meta, pl, pl_id, id_gen, ref_groups);
        }
    }

    if let Some(sl) = &spectrum.scan_list {
        let sl_id = id_gen.alloc();

        let attrs = assign_attributes(sl, TagId::ScanList, sl_id, spectrum_id);
        meta.push_assigned_attributes_as_cv_params(TagId::ScanList, sl_id, spectrum_id, attrs);

        meta.extend_tagged_ids(TagId::ScanList, sl_id, spectrum_id, &sl.cv_params);

        flatten_scan_list_ids(meta, sl, sl_id, id_gen, ref_groups);
    }

    if let Some(pl) = &spectrum.precursor_list {
        let pl_id = id_gen.alloc();

        let attrs = assign_attributes(pl, TagId::PrecursorList, pl_id, spectrum_id);
        meta.push_assigned_attributes_as_cv_params(TagId::PrecursorList, pl_id, spectrum_id, attrs);

        flatten_precursor_list_ids(meta, pl, pl_id, id_gen, ref_groups);
    }

    if let Some(pl) = &spectrum.product_list {
        let pl_id = id_gen.alloc();

        let attrs = assign_attributes(pl, TagId::ProductList, pl_id, spectrum_id);
        meta.push_assigned_attributes_as_cv_params(TagId::ProductList, pl_id, spectrum_id, attrs);

        flatten_product_list_ids(meta, pl, pl_id, id_gen, ref_groups);
    }

    if let Some(bal) = &spectrum.binary_data_array_list {
        let bal_id = id_gen.alloc();

        let attrs = assign_attributes(bal, TagId::BinaryDataArrayList, bal_id, spectrum_id);
        meta.push_assigned_attributes_as_cv_params(
            TagId::BinaryDataArrayList,
            bal_id,
            spectrum_id,
            attrs,
        );

        for ba in &bal.binary_data_arrays {
            let ba_id = id_gen.alloc();

            meta.push_schema_attributes(TagId::BinaryDataArray, ba_id, bal_id, ba);

            meta.extend_ref_group_cv_params_ids(
                TagId::BinaryDataArray,
                ba_id,
                bal_id,
                &ba.referenceable_param_group_refs,
                ref_groups,
            );

            extend_binary_data_array_cv_params_ids(
                meta,
                ba_id,
                bal_id,
                ba,
                x_accession_tail,
                y_accession_tail,
                f32_compress,
            );

            let _ = (x_store_f64, y_store_f64);
        }
    }
}

/// <chromatogram>
fn flatten_chromatogram_metadata_into(
    meta: &mut MetaAcc<'_>,
    chrom: &Chromatogram,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    chrom_id: u32,
    parent_owner_id: u32,
    id_gen: &mut NodeIdGen,
    x_accession_tail: u32,
    y_accession_tail: u32,
    x_store_f64: bool,
    y_store_f64: bool,
    f32_compress: bool,
) {
    let attrs = assign_attributes(chrom, TagId::Chromatogram, chrom_id, parent_owner_id);
    meta.push_assigned_attributes_as_cv_params(
        TagId::Chromatogram,
        chrom_id,
        parent_owner_id,
        attrs,
    );

    meta.extend_ref_group_cv_params_ids(
        TagId::Chromatogram,
        chrom_id,
        parent_owner_id,
        &chrom.referenceable_param_group_refs,
        ref_groups,
    );

    meta.extend_tagged_ids(
        TagId::Chromatogram,
        chrom_id,
        parent_owner_id,
        &chrom.cv_params,
    );

    if let Some(p) = &chrom.precursor {
        flatten_precursor_ids(meta, p, chrom_id, id_gen, ref_groups);
    }

    if let Some(p) = &chrom.product {
        flatten_product_ids(meta, p, chrom_id, id_gen, ref_groups);
    }

    if let Some(bal) = &chrom.binary_data_array_list {
        let bal_id = id_gen.alloc();

        let attrs = assign_attributes(bal, TagId::BinaryDataArrayList, bal_id, chrom_id);
        meta.push_assigned_attributes_as_cv_params(
            TagId::BinaryDataArrayList,
            bal_id,
            chrom_id,
            attrs,
        );

        for ba in &bal.binary_data_arrays {
            let ba_id = id_gen.alloc();

            meta.push_schema_attributes(TagId::BinaryDataArray, ba_id, bal_id, ba);

            meta.extend_ref_group_cv_params_ids(
                TagId::BinaryDataArray,
                ba_id,
                bal_id,
                &ba.referenceable_param_group_refs,
                ref_groups,
            );

            extend_binary_data_array_cv_params_ids(
                meta,
                ba_id,
                bal_id,
                ba,
                x_accession_tail,
                y_accession_tail,
                f32_compress,
            );

            let _ = (x_store_f64, y_store_f64);
        }
    }
}

/// <cvList>
fn build_global_meta_items(
    mzml: &MzML,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    id_gen: &mut NodeIdGen,
) -> (Vec<GlobalMetaItem>, GlobalCounts) {
    let mut items: Vec<GlobalMetaItem> = Vec::new();

    {
        let fd = &mzml.file_description;

        let mut out = Vec::new();
        let mut tags = Vec::new();
        let mut owners = Vec::new();
        let mut parents = Vec::new();
        let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

        // <fileDescription>
        let file_desc_id = id_gen.alloc();

        // <fileContent>
        let file_content_id = id_gen.alloc();

        meta.extend_ref_group_cv_params_ids(
            TagId::FileContent,
            file_content_id,
            file_desc_id,
            &fd.file_content.referenceable_param_group_refs,
            ref_groups,
        );

        meta.extend_tagged_ids(
            TagId::FileContent,
            file_content_id,
            file_desc_id,
            &fd.file_content.cv_params,
        );

        let sfl_id = id_gen.alloc();

        meta.push_attr_usize_tagged_ids(
            TagId::SourceFileList,
            sfl_id,
            file_desc_id,
            ACC_ATTR_COUNT,
            Some(fd.source_file_list.source_file.len() as u32),
        );

        for sf in &fd.source_file_list.source_file {
            let sf_id = id_gen.alloc();

            meta.push_attr_string_tagged_ids(
                TagId::SourceFile,
                sf_id,
                sfl_id,
                ACC_ATTR_ID,
                sf.id.as_str(),
            );

            meta.push_attr_string_tagged_ids(
                TagId::SourceFile,
                sf_id,
                sfl_id,
                ACC_ATTR_NAME,
                sf.name.as_str(),
            );

            meta.push_attr_string_tagged_ids(
                TagId::SourceFile,
                sf_id,
                sfl_id,
                ACC_ATTR_LOCATION,
                sf.location.as_str(),
            );

            meta.extend_ref_group_cv_params_ids(
                TagId::SourceFile,
                sf_id,
                sfl_id,
                &sf.referenceable_param_group_ref,
                ref_groups,
            );

            meta.extend_tagged_ids(TagId::SourceFile, sf_id, sfl_id, &sf.cv_param);
        }

        for c in &fd.contacts {
            let contact_id = id_gen.alloc();

            meta.extend_ref_group_cv_params_ids(
                TagId::Contact,
                contact_id,
                file_desc_id,
                &c.referenceable_param_group_refs,
                ref_groups,
            );

            meta.extend_tagged_ids(TagId::Contact, contact_id, file_desc_id, &c.cv_params);
        }

        items.push(GlobalMetaItem {
            cvs: out,
            tags,
            owners,
            parents,
        });
    }

    let n_file_description = 1u32;
    let n_run = 1u32;

    {
        let run = &mzml.run;

        let mut out: Vec<CvParam> = Vec::new();
        let mut tags: Vec<u8> = Vec::new();
        let mut owners: Vec<u32> = Vec::new();
        let mut parents: Vec<u32> = Vec::new();
        let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

        let run_id = id_gen.alloc();

        if !run.id.is_empty() {
            meta.push_attr_string_tagged_ids(TagId::Run, run_id, 0, ACC_ATTR_ID, run.id.as_str());
        }

        if let Some(v) = run.start_time_stamp.as_deref() {
            meta.push_attr_string_tagged_ids(TagId::Run, run_id, 0, ACC_ATTR_START_TIME_STAMP, v);
        }

        if let Some(v) = run.default_instrument_configuration_ref.as_deref() {
            meta.push_attr_string_tagged_ids(
                TagId::Run,
                run_id,
                0,
                ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
                v,
            );
        }

        if let Some(v) = run.default_source_file_ref.as_deref() {
            meta.push_attr_string_tagged_ids(
                TagId::Run,
                run_id,
                0,
                ACC_ATTR_DEFAULT_SOURCE_FILE_REF,
                v,
            );
        }

        if let Some(v) = run.sample_ref.as_deref() {
            meta.push_attr_string_tagged_ids(TagId::Run, run_id, 0, ACC_ATTR_SAMPLE_REF, v);
        }

        if let Some(sfrl) = &run.source_file_ref_list {
            let sfrl_id = id_gen.alloc();

            meta.push_attr_usize_tagged_ids(
                TagId::SourceFileRefList,
                sfrl_id,
                run_id,
                ACC_ATTR_COUNT,
                Some(sfrl.source_file_refs.len() as u32),
            );

            for sref in &sfrl.source_file_refs {
                let sref_id = id_gen.alloc();
                meta.push_attr_string_tagged_ids(
                    TagId::SourceFileRef,
                    sref_id,
                    sfrl_id,
                    ACC_ATTR_REF,
                    sref.r#ref.as_str(),
                );
            }
        }

        for r in &run.referenceable_param_group_refs {
            let rgr_id = id_gen.alloc();
            meta.push_attr_string_tagged_ids(
                TagId::ReferenceableParamGroupRef,
                rgr_id,
                run_id,
                ACC_ATTR_REF,
                r.r#ref.as_str(),
            );
        }

        if !run.cv_params.is_empty() {
            meta.extend_tagged_ids(TagId::Run, run_id, 0, &run.cv_params);
        }

        items.push(GlobalMetaItem {
            cvs: out,
            tags,
            owners,
            parents,
        });
    }

    let ref_start = items.len();
    if let Some(rpgl) = &mzml.referenceable_param_group_list {
        for g in &rpgl.referenceable_param_groups {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let g_id = id_gen.alloc();

            meta.push_attr_string_tagged_ids(
                TagId::ReferenceableParamGroup,
                g_id,
                0,
                ACC_ATTR_ID,
                g.id.as_str(),
            );

            meta.extend_tagged_ids(TagId::ReferenceableParamGroup, g_id, 0, &g.cv_params);

            items.push(GlobalMetaItem {
                cvs: out,
                tags,
                owners,
                parents,
            });
        }
    }
    let n_ref_param_groups = (items.len() - ref_start) as u32;

    let samples_start = items.len();
    if let Some(sl) = &mzml.sample_list {
        for s in &sl.samples {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let sample_id = id_gen.alloc();

            meta.push_attr_string_tagged_ids(
                TagId::Sample,
                sample_id,
                0,
                ACC_ATTR_ID,
                s.id.as_str(),
            );

            meta.push_attr_string_tagged_ids(
                TagId::Sample,
                sample_id,
                0,
                ACC_ATTR_NAME,
                s.name.as_str(),
            );

            if let Some(r) = &s.referenceable_param_group_ref {
                meta.extend_ref_group_cv_params_ids(
                    TagId::Sample,
                    sample_id,
                    0,
                    slice::from_ref(r),
                    ref_groups,
                );
            }

            meta.extend_tagged_ids(TagId::Sample, sample_id, 0, &[]);

            items.push(GlobalMetaItem {
                cvs: out,
                tags,
                owners,
                parents,
            });
        }
    }
    let n_samples = (items.len() - samples_start) as u32;

    let instr_start = items.len();
    if let Some(il) = &mzml.instrument_list {
        for ic in &il.instrument {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let inst_id = id_gen.alloc();

            meta.push_attr_string_tagged_ids(
                TagId::Instrument,
                inst_id,
                0,
                ACC_ATTR_ID,
                ic.id.as_str(),
            );

            meta.extend_ref_group_cv_params_ids(
                TagId::Instrument,
                inst_id,
                0,
                &ic.referenceable_param_group_ref,
                ref_groups,
            );

            meta.extend_tagged_ids(TagId::Instrument, inst_id, 0, &ic.cv_param);

            if let Some(cl) = &ic.component_list {
                for s in &cl.source {
                    let cid = id_gen.alloc();
                    meta.push_attr_usize_tagged_ids(
                        TagId::ComponentSource,
                        cid,
                        inst_id,
                        ACC_ATTR_ORDER,
                        s.order,
                    );

                    meta.extend_ref_group_cv_params_ids(
                        TagId::ComponentSource,
                        cid,
                        inst_id,
                        &s.referenceable_param_group_ref,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(TagId::ComponentSource, cid, inst_id, &s.cv_param);
                }

                for a in &cl.analyzer {
                    let cid = id_gen.alloc();

                    meta.push_attr_usize_tagged_ids(
                        TagId::ComponentAnalyzer,
                        cid,
                        inst_id,
                        ACC_ATTR_ORDER,
                        a.order,
                    );

                    meta.extend_ref_group_cv_params_ids(
                        TagId::ComponentAnalyzer,
                        cid,
                        inst_id,
                        &a.referenceable_param_group_ref,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(TagId::ComponentAnalyzer, cid, inst_id, &a.cv_param);
                }

                for d in &cl.detector {
                    let cid = id_gen.alloc();

                    meta.push_attr_usize_tagged_ids(
                        TagId::ComponentDetector,
                        cid,
                        inst_id,
                        ACC_ATTR_ORDER,
                        d.order,
                    );

                    meta.extend_ref_group_cv_params_ids(
                        TagId::ComponentDetector,
                        cid,
                        inst_id,
                        &d.referenceable_param_group_ref,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(TagId::ComponentDetector, cid, inst_id, &d.cv_param);
                }
            }

            items.push(GlobalMetaItem {
                cvs: out,
                tags,
                owners,
                parents,
            });
        }
    }
    let n_instrument_configs = (items.len() - instr_start) as u32;

    let sw_start = items.len();
    if let Some(sw) = &mzml.software_list {
        for s in &sw.software {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let sw_id = id_gen.alloc();

            meta.push_attr_string_tagged_ids(TagId::Software, sw_id, 0, ACC_ATTR_ID, s.id.as_str());

            let ver = s
                .version
                .as_deref()
                .or_else(|| s.software_param.first().and_then(|p| p.version.as_deref()));
            if let Some(ver) = ver {
                meta.push_attr_string_tagged_ids(TagId::Software, sw_id, 0, ACC_ATTR_VERSION, ver);
            }

            for p in &s.software_param {
                let sp_id = id_gen.alloc();

                meta.push_tagged_ids(
                    TagId::SoftwareParam,
                    sp_id,
                    sw_id,
                    CvParam {
                        cv_ref: p.cv_ref.clone(),
                        accession: Some(p.accession.clone()),
                        name: p.name.clone(),
                        value: Some(String::new()),
                        unit_cv_ref: None,
                        unit_name: None,
                        unit_accession: None,
                    },
                );
            }

            meta.extend_tagged_ids(TagId::Software, sw_id, 0, &s.cv_param);

            items.push(GlobalMetaItem {
                cvs: out,
                tags,
                owners,
                parents,
            });
        }
    }
    let n_software = (items.len() - sw_start) as u32;

    let dp_start = items.len();
    if let Some(dpl) = &mzml.data_processing_list {
        for dp in &dpl.data_processing {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let dp_id = id_gen.alloc();

            meta.push_attr_string_tagged_ids(
                TagId::DataProcessing,
                dp_id,
                0,
                ACC_ATTR_ID,
                dp.id.as_str(),
            );

            for m in &dp.processing_method {
                let pm_id = id_gen.alloc();

                meta.extend_ref_group_cv_params_ids(
                    TagId::ProcessingMethod,
                    pm_id,
                    dp_id,
                    &m.referenceable_param_group_ref,
                    ref_groups,
                );

                meta.extend_tagged_ids(TagId::ProcessingMethod, pm_id, dp_id, &m.cv_param);
            }

            items.push(GlobalMetaItem {
                cvs: out,
                tags,
                owners,
                parents,
            });
        }
    }
    let n_data_processing = (items.len() - dp_start) as u32;

    let acq_start = items.len();
    if let Some(ssl) = &mzml.scan_settings_list {
        for ss in &ssl.scan_settings {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let ss_id = id_gen.alloc();

            if let Some(id) = ss.id.as_deref() {
                meta.push_attr_string_tagged_ids(TagId::ScanSettings, ss_id, 0, ACC_ATTR_ID, id);
            }

            if let Some(icr) = ss.instrument_configuration_ref.as_deref() {
                meta.push_attr_string_tagged_ids(
                    TagId::ScanSettings,
                    ss_id,
                    0,
                    ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
                    icr,
                );
            }

            if let Some(sfrl) = &ss.source_file_ref_list {
                let sfrl_id = id_gen.alloc();

                meta.push_attr_usize_tagged_ids(
                    TagId::SourceFileRefList,
                    sfrl_id,
                    ss_id,
                    ACC_ATTR_COUNT,
                    Some(sfrl.source_file_refs.len() as u32),
                );

                for sref in &sfrl.source_file_refs {
                    let sfr_id = id_gen.alloc();
                    meta.push_attr_string_tagged_ids(
                        TagId::SourceFileRef,
                        sfr_id,
                        sfrl_id,
                        ACC_ATTR_REF,
                        sref.r#ref.as_str(),
                    );
                }
            }

            meta.extend_ref_group_cv_params_ids(
                TagId::ScanSettings,
                ss_id,
                0,
                &ss.referenceable_param_group_refs,
                ref_groups,
            );

            meta.extend_tagged_ids(TagId::ScanSettings, ss_id, 0, &ss.cv_params);

            if let Some(tl) = &ss.target_list {
                for t in &tl.targets {
                    let tgt_id = id_gen.alloc();

                    meta.extend_ref_group_cv_params_ids(
                        TagId::Target,
                        tgt_id,
                        ss_id,
                        &t.referenceable_param_group_refs,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(TagId::Target, tgt_id, ss_id, &t.cv_params);
                }
            }

            items.push(GlobalMetaItem {
                cvs: out,
                tags,
                owners,
                parents,
            });
        }
    }
    let n_acquisition_settings = (items.len() - acq_start) as u32;

    let cv_start = items.len();
    if let Some(cl) = &mzml.cv_list {
        let cv_count = cl.cv.len() as u32;

        if cv_count != 0 {
            let cv_list_id = id_gen.alloc();

            for (i, cv) in cl.cv.iter().enumerate() {
                let mut out = Vec::new();
                let mut tags = Vec::new();
                let mut owners = Vec::new();
                let mut parents = Vec::new();
                let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

                if i == 0 {
                    meta.push_attr_usize_tagged_ids(
                        TagId::CvList,
                        cv_list_id,
                        0,
                        ACC_ATTR_COUNT,
                        Some(cv_count),
                    );
                }

                let cv_id = id_gen.alloc();

                meta.push_attr_string_tagged_ids(
                    TagId::Cv,
                    cv_id,
                    cv_list_id,
                    ACC_ATTR_LABEL,
                    cv.id.as_str(),
                );

                if let Some(v) = cv.full_name.as_deref() {
                    if !v.is_empty() {
                        meta.push_attr_string_tagged_ids(
                            TagId::Cv,
                            cv_id,
                            cv_list_id,
                            ACC_ATTR_CV_FULL_NAME,
                            v,
                        );
                    }
                }

                if let Some(v) = cv.version.as_deref() {
                    if !v.is_empty() {
                        meta.push_attr_string_tagged_ids(
                            TagId::Cv,
                            cv_id,
                            cv_list_id,
                            ACC_ATTR_CV_VERSION,
                            v,
                        );
                    }
                }

                if let Some(v) = cv.uri.as_deref() {
                    if !v.is_empty() {
                        meta.push_attr_string_tagged_ids(
                            TagId::Cv,
                            cv_id,
                            cv_list_id,
                            ACC_ATTR_CV_URI,
                            v,
                        );
                    }
                }

                items.push(GlobalMetaItem {
                    cvs: out,
                    tags,
                    owners,
                    parents,
                });
            }
        }
    }

    let n_cvs = (items.len() - cv_start) as u32;

    (
        items,
        GlobalCounts {
            n_file_description,
            n_run,
            n_ref_param_groups,
            n_samples,
            n_instrument_configs,
            n_software,
            n_data_processing,
            n_acquisition_settings,
            n_cvs,
        },
    )
}

#[inline]
fn pack_cv_param(
    tag_id: u8,
    owner_id: u32,
    parent_owner_id: u32,
    cv: &CvParam,
    tag_ids: &mut Vec<u8>,
    owner_ids: &mut Vec<u32>,
    parent_indices: &mut Vec<u32>,
    ref_codes: &mut Vec<u8>,
    accession_numbers: &mut Vec<u32>,
    unit_ref_codes: &mut Vec<u8>,
    unit_accession_numbers: &mut Vec<u32>,
    value_kinds: &mut Vec<u8>,
    value_indices: &mut Vec<u32>,
    numeric_values: &mut Vec<f64>,
    string_offsets: &mut Vec<u32>,
    string_lengths: &mut Vec<u32>,
    string_bytes: &mut Vec<u8>,
    numeric_index: &mut u32,
    string_index: &mut u32,
) {
    tag_ids.push(tag_id);
    owner_ids.push(owner_id);
    parent_indices.push(parent_owner_id);

    let cv_ref = cv_ref_from_accession(cv.accession.as_deref()).or(cv.cv_ref.as_deref());
    ref_codes.push(cv_ref_code_from_str(cv_ref));

    accession_numbers.push(parse_accession_tail(cv.accession.as_deref()));

    let unit_ref =
        cv_ref_from_accession(cv.unit_accession.as_deref()).or(cv.unit_cv_ref.as_deref());
    unit_ref_codes.push(cv_ref_code_from_str(unit_ref));

    unit_accession_numbers.push(parse_accession_tail(cv.unit_accession.as_deref()));

    let (kind, idx) = match cv.value.as_deref() {
        None | Some("") => (2u8, 0u32),
        Some(val) => {
            if let Ok(num) = val.parse::<f64>() {
                let i = *numeric_index;
                numeric_values.push(num);
                *numeric_index += 1;
                (0u8, i)
            } else {
                let i = *string_index;
                let bytes = val.as_bytes();
                let off = string_bytes.len() as u32;
                let len = bytes.len() as u32;

                string_bytes.extend_from_slice(bytes);
                string_offsets.push(off);
                string_lengths.push(len);

                *string_index += 1;
                (1u8, i)
            }
        }
    };

    value_kinds.push(kind);
    value_indices.push(idx);
}

#[inline]
fn cv_ref_from_accession<'a>(acc: Option<&'a str>) -> Option<&'a str> {
    acc.and_then(|s| s.split_once(':').map(|(p, _)| p))
}

/// <cvParam>
fn pack_meta_streaming<T, F>(items: &[T], mut fill: F) -> PackedMeta
where
    F: FnMut(&mut MetaAcc<'_>, &T),
{
    let item_count = items.len();

    let mut index_offsets = Vec::with_capacity(item_count + 1);

    let mut owner_ids: Vec<u32> = Vec::new();
    let mut parent_indices: Vec<u32> = Vec::new();

    let mut tag_ids: Vec<u8> = Vec::new();
    let mut ref_codes: Vec<u8> = Vec::new();
    let mut accession_numbers: Vec<u32> = Vec::new();
    let mut unit_ref_codes: Vec<u8> = Vec::new();
    let mut unit_accession_numbers: Vec<u32> = Vec::new();
    let mut value_kinds: Vec<u8> = Vec::new();
    let mut value_indices: Vec<u32> = Vec::new();

    let mut numeric_values: Vec<f64> = Vec::new();
    let mut string_offsets: Vec<u32> = Vec::new();
    let mut string_lengths: Vec<u32> = Vec::new();
    let mut string_bytes: Vec<u8> = Vec::new();

    let mut scratch: Vec<CvParam> = Vec::with_capacity(256);
    let mut scratch_tags: Vec<u8> = Vec::with_capacity(256);
    let mut scratch_owners: Vec<u32> = Vec::with_capacity(256);
    let mut scratch_parents: Vec<u32> = Vec::with_capacity(256);

    let mut numeric_index: u32 = 0;
    let mut string_index: u32 = 0;
    let mut meta_index: u32 = 0;

    index_offsets.push(0);

    for item in items {
        scratch.clear();
        scratch_tags.clear();
        scratch_owners.clear();
        scratch_parents.clear();

        {
            let mut meta = MetaAcc::new(
                &mut scratch,
                &mut scratch_tags,
                &mut scratch_owners,
                &mut scratch_parents,
            );
            fill(&mut meta, item);
        }

        debug_assert_eq!(scratch.len(), scratch_tags.len());
        debug_assert_eq!(scratch.len(), scratch_owners.len());
        debug_assert_eq!(scratch.len(), scratch_parents.len());

        for i in 0..scratch.len() {
            pack_cv_param(
                scratch_tags[i],
                scratch_owners[i],
                scratch_parents[i],
                &scratch[i],
                &mut tag_ids,
                &mut owner_ids,
                &mut parent_indices,
                &mut ref_codes,
                &mut accession_numbers,
                &mut unit_ref_codes,
                &mut unit_accession_numbers,
                &mut value_kinds,
                &mut value_indices,
                &mut numeric_values,
                &mut string_offsets,
                &mut string_lengths,
                &mut string_bytes,
                &mut numeric_index,
                &mut string_index,
            );
            meta_index += 1;
        }

        index_offsets.push(meta_index);
    }

    PackedMeta {
        index_offsets,
        owner_ids,
        parent_indices,
        tag_ids,
        ref_codes,
        accession_numbers,
        unit_ref_codes,
        unit_accession_numbers,
        value_kinds,
        value_indices,
        numeric_values,
        string_offsets,
        string_lengths,
        string_bytes,
    }
}

fn pack_meta_slices<T, F>(items: &[T], meta_of: F) -> PackedMeta
where
    F: Fn(&T) -> (&[CvParam], &[u8], &[u32], &[u32]),
{
    let item_count = items.len();

    let mut total_meta_count = 0usize;
    for item in items {
        total_meta_count += meta_of(item).0.len();
    }

    let mut index_offsets = Vec::with_capacity(item_count + 1);

    let mut owner_ids = Vec::with_capacity(total_meta_count);
    let mut parent_indices = Vec::with_capacity(total_meta_count);

    let mut tag_ids = Vec::with_capacity(total_meta_count);
    let mut ref_codes = Vec::with_capacity(total_meta_count);
    let mut accession_numbers = Vec::with_capacity(total_meta_count);
    let mut unit_ref_codes = Vec::with_capacity(total_meta_count);
    let mut unit_accession_numbers = Vec::with_capacity(total_meta_count);
    let mut value_kinds = Vec::with_capacity(total_meta_count);
    let mut value_indices = Vec::with_capacity(total_meta_count);

    let mut numeric_values = Vec::with_capacity(total_meta_count);
    let mut string_offsets = Vec::with_capacity(total_meta_count);
    let mut string_lengths = Vec::with_capacity(total_meta_count);
    let mut string_bytes = Vec::new();

    let mut numeric_index: u32 = 0;
    let mut string_index: u32 = 0;
    let mut meta_index: u32 = 0;

    index_offsets.push(0);

    for item in items {
        let (xs, ts, os, ps) = meta_of(item);
        debug_assert_eq!(xs.len(), ts.len());
        debug_assert_eq!(xs.len(), os.len());
        debug_assert_eq!(xs.len(), ps.len());

        for i in 0..xs.len() {
            pack_cv_param(
                ts[i],
                os[i],
                ps[i],
                &xs[i],
                &mut tag_ids,
                &mut owner_ids,
                &mut parent_indices,
                &mut ref_codes,
                &mut accession_numbers,
                &mut unit_ref_codes,
                &mut unit_accession_numbers,
                &mut value_kinds,
                &mut value_indices,
                &mut numeric_values,
                &mut string_offsets,
                &mut string_lengths,
                &mut string_bytes,
                &mut numeric_index,
                &mut string_index,
            );
            meta_index += 1;
        }

        index_offsets.push(meta_index);
    }

    PackedMeta {
        index_offsets,
        owner_ids,
        parent_indices,
        tag_ids,
        ref_codes,
        accession_numbers,
        unit_ref_codes,
        unit_accession_numbers,
        value_kinds,
        value_indices,
        numeric_values,
        string_offsets,
        string_lengths,
        string_bytes,
    }
}

fn packed_meta_byte_len(meta: &PackedMeta) -> usize {
    meta.index_offsets.len() * 4
        + meta.owner_ids.len() * 4
        + meta.parent_indices.len() * 4
        + meta.tag_ids.len()
        + meta.ref_codes.len()
        + meta.accession_numbers.len() * 4
        + meta.unit_ref_codes.len()
        + meta.unit_accession_numbers.len() * 4
        + meta.value_kinds.len()
        + meta.value_indices.len() * 4
        + meta.numeric_values.len() * 8
        + meta.string_offsets.len() * 4
        + meta.string_lengths.len() * 4
        + meta.string_bytes.len()
}

fn write_packed_meta_into(buf: &mut Vec<u8>, meta: &PackedMeta) {
    write_u32_slice_le(buf, &meta.index_offsets);
    write_u32_slice_le(buf, &meta.owner_ids);
    write_u32_slice_le(buf, &meta.parent_indices);

    buf.extend_from_slice(&meta.tag_ids);
    buf.extend_from_slice(&meta.ref_codes);

    write_u32_slice_le(buf, &meta.accession_numbers);

    buf.extend_from_slice(&meta.unit_ref_codes);

    write_u32_slice_le(buf, &meta.unit_accession_numbers);

    buf.extend_from_slice(&meta.value_kinds);

    write_u32_slice_le(buf, &meta.value_indices);

    write_f64_slice_le(buf, &meta.numeric_values);

    write_u32_slice_le(buf, &meta.string_offsets);
    write_u32_slice_le(buf, &meta.string_lengths);
    buf.extend_from_slice(&meta.string_bytes);
}

fn write_packed_meta_bytes(meta: &PackedMeta) -> Vec<u8> {
    let mut buf = Vec::with_capacity(packed_meta_byte_len(meta));
    write_packed_meta_into(&mut buf, meta);
    buf
}

fn write_global_meta_bytes(counts: &GlobalCounts, meta: &PackedMeta) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 * 4 + packed_meta_byte_len(meta));

    write_u32_le(&mut buf, counts.n_file_description);
    write_u32_le(&mut buf, counts.n_run);
    write_u32_le(&mut buf, counts.n_ref_param_groups);
    write_u32_le(&mut buf, counts.n_samples);
    write_u32_le(&mut buf, counts.n_instrument_configs);
    write_u32_le(&mut buf, counts.n_software);
    write_u32_le(&mut buf, counts.n_data_processing);
    write_u32_le(&mut buf, counts.n_acquisition_settings);
    write_u32_le(&mut buf, counts.n_cvs);

    write_packed_meta_into(&mut buf, meta);
    buf
}

fn append_aligned_8(output: &mut Vec<u8>, bytes: &[u8]) -> u64 {
    let aligned = align_to_8(output.len());
    if aligned > output.len() {
        output.resize(aligned, 0);
    }
    let offset = output.len() as u64;
    output.extend_from_slice(bytes);
    offset
}

/// <binaryDataArray>
fn array_ref<'a>(ba: &'a BinaryDataArray) -> Option<ArrayRef<'a>> {
    let bin = ba.binary.as_ref()?;

    match bin {
        BinaryData::F16(v) => Some(ArrayRef::F16(v.as_slice())),
        BinaryData::I16(v) => Some(ArrayRef::I16(v.as_slice())),
        BinaryData::I32(v) => Some(ArrayRef::I32(v.as_slice())),
        BinaryData::I64(v) => Some(ArrayRef::I64(v.as_slice())),
        BinaryData::F32(v) => Some(ArrayRef::F32(v.as_slice())),
        BinaryData::F64(v) => Some(ArrayRef::F64(v.as_slice())),
    }
}

#[inline]
fn parse_accession_tail(accession: Option<&str>) -> u32 {
    let s = accession.unwrap_or("");
    let tail = match s.rsplit_once(':') {
        Some((_, t)) => t,
        None => s,
    };

    let mut v: u32 = 0;
    let mut saw_digit = false;

    for b in tail.bytes() {
        if (b'0'..=b'9').contains(&b) {
            saw_digit = true;
            let d = (b - b'0') as u32;
            match v.checked_mul(10).and_then(|x| x.checked_add(d)) {
                Some(n) => v = n,
                None => return 0,
            }
        }
    }

    if saw_digit { v } else { 0 }
}

#[inline]
fn align_to_8(x: usize) -> usize {
    (x + 7) & !7
}

#[inline]
fn write_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn write_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn write_f64_le(buf: &mut Vec<u8>, value: f64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn write_f32_le(buf: &mut Vec<u8>, value: f32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn write_f64_as_f32(buf: &mut Vec<u8>, value: f64) {
    buf.extend_from_slice(&(value as f32).to_le_bytes());
}

#[inline]
fn set_u8_at(buf: &mut [u8], offset: usize, value: u8) {
    buf[offset] = value;
}

#[inline]
fn set_u32_at(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn set_u64_at(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn bda_declared_is_f64(ba: &BinaryDataArray) -> Option<bool> {
    if let Some(nt) = ba.numeric_type.as_ref() {
        return match nt {
            NumericType::Float64 => Some(true),
            NumericType::Float32 => Some(false),
            _ => None,
        };
    }

    let mut saw32 = false;
    let mut saw64 = false;

    for cv in &ba.cv_params {
        match parse_accession_tail(cv.accession.as_deref()) {
            ACC_32BIT_FLOAT => saw32 = true,
            ACC_64BIT_FLOAT => saw64 = true,
            _ => {}
        }
        if saw32 && saw64 {
            break;
        }
    }

    match (saw32, saw64) {
        (true, false) => Some(false),
        (false, true) => Some(true),
        _ => None,
    }
}

/// <mzML>
pub fn encode(mzml: &MzML, compression_level: u8, f32_compress: bool) -> Vec<u8> {
    assert!(compression_level <= 22);

    #[inline]
    fn fix_attr_values(out: &mut Vec<CvParam>) {
        for cv in out.iter_mut() {
            if cv.cv_ref.as_deref() == Some(CV_REF_ATTR) {
                let empty_val = cv.value.as_deref().map_or(true, |s| s.is_empty());
                if empty_val && !cv.name.is_empty() {
                    cv.value = Some(mem::take(&mut cv.name));
                }
            }
        }
    }

    #[inline]
    fn entry_dtype_from_writer_dtype(writer_dtype: u8) -> u8 {
        match writer_dtype {
            1 => 2,
            2 => 1,
            _ => writer_dtype,
        }
    }

    let compress_meta = compression_level != 0;
    let do_shuffle = compress_meta;

    let array_filter_id = if do_shuffle {
        ARRAY_FILTER_BYTE_SHUFFLE
    } else {
        ARRAY_FILTER_NONE
    };

    let compression_codec: u8 = if compression_level == 0 { 0 } else { 1 };

    let run = &mzml.run;

    let spectra: &[Spectrum] = run
        .spectrum_list
        .as_ref()
        .map(|sl| sl.spectra.as_slice())
        .unwrap_or(&[]);

    let chromatograms: &[Chromatogram] = run
        .chromatogram_list
        .as_ref()
        .map(|cl| cl.chromatograms.as_slice())
        .unwrap_or(&[]);

    let spectrum_count = spectra.len() as u32;
    let chrom_count = chromatograms.len() as u32;

    #[inline]
    fn array_type_tail(ba: &BinaryDataArray) -> u32 {
        for cv in &ba.cv_params {
            let tail = parse_accession_tail(cv.accession.as_deref());
            if tail == ACC_MZ_ARRAY || tail == ACC_INTENSITY_ARRAY || tail == ACC_TIME_ARRAY {
                return tail;
            }
        }
        for cv in &ba.cv_params {
            let tail = parse_accession_tail(cv.accession.as_deref());
            if tail != 0 {
                let name = cv.name.to_ascii_lowercase();
                if name.contains(" array") {
                    return tail;
                }
            }
        }
        0
    }

    #[inline]
    fn store_as_f64(ba: &BinaryDataArray, arr: ArrayRef<'_>, f32_compress: bool) -> bool {
        if f32_compress {
            return false;
        }
        bda_declared_is_f64(ba).unwrap_or(matches!(arr, ArrayRef::F64(_)))
    }

    #[inline]
    fn dtype_of(ba: &BinaryDataArray, arr: ArrayRef<'_>, f32_compress: bool) -> u8 {
        match arr {
            ArrayRef::F16(_) => DTYPE_F16,
            ArrayRef::I16(_) => DTYPE_I16,
            ArrayRef::I32(_) => DTYPE_I32,
            ArrayRef::I64(_) => DTYPE_I64,
            ArrayRef::F32(_) | ArrayRef::F64(_) => {
                if store_as_f64(ba, arr, f32_compress) {
                    DTYPE_F64
                } else {
                    DTYPE_F32
                }
            }
        }
    }

    let ref_groups = build_ref_group_map(mzml);
    let mut id_gen = NodeIdGen::new();

    let spectrum_list_owner_id: u32 = if run.spectrum_list.is_some() {
        id_gen.alloc()
    } else {
        0
    };
    let chromatogram_list_owner_id: u32 = if run.chromatogram_list.is_some() {
        id_gen.alloc()
    } else {
        0
    };

    let (mut global_items, global_counts) = build_global_meta_items(mzml, &ref_groups, &mut id_gen);
    for item in &mut global_items {
        fix_attr_values(&mut item.cvs);
    }

    let mut spec_i: usize = 0;
    let spectrum_meta = pack_meta_streaming(spectra, |meta, s| {
        let idx = spec_i;
        spec_i += 1;

        if idx == 0 {
            if let Some(sl) = run.spectrum_list.as_ref() {
                if spectrum_list_owner_id != 0 {
                    meta.push_schema_attributes(TagId::SpectrumList, spectrum_list_owner_id, 0, sl);
                }
            }
        }

        let spectrum_id = id_gen.alloc();

        flatten_spectrum_metadata_into_owned(
            meta,
            s,
            &ref_groups,
            spectrum_id,
            0,
            &mut id_gen,
            ACC_MZ_ARRAY,
            ACC_INTENSITY_ARRAY,
            false,
            false,
            f32_compress,
        );

        fix_attr_values(meta.out);
    });

    let mut chrom_i: usize = 0;
    let chromatogram_meta = pack_meta_streaming(chromatograms, |meta, c| {
        let idx = chrom_i;
        chrom_i += 1;

        if idx == 0 {
            if let Some(cl) = run.chromatogram_list.as_ref() {
                if chromatogram_list_owner_id != 0 {
                    meta.push_schema_attributes(
                        TagId::ChromatogramList,
                        chromatogram_list_owner_id,
                        0,
                        cl,
                    );
                }
            }
        }

        let chrom_id = id_gen.alloc();

        flatten_chromatogram_metadata_into(
            meta,
            c,
            &ref_groups,
            chrom_id,
            0,
            &mut id_gen,
            ACC_TIME_ARRAY,
            ACC_INTENSITY_ARRAY,
            false,
            false,
            f32_compress,
        );

        fix_attr_values(meta.out);
    });

    let global_meta = pack_meta_slices(&global_items, |m| {
        (
            m.cvs.as_slice(),
            m.tags.as_slice(),
            m.owners.as_slice(),
            m.parents.as_slice(),
        )
    });

    let spec_meta_count = spectrum_meta.ref_codes.len() as u32;
    let spec_num_count = spectrum_meta.numeric_values.len() as u32;
    let spec_str_count = spectrum_meta.string_offsets.len() as u32;

    let chrom_meta_count = chromatogram_meta.ref_codes.len() as u32;
    let chrom_num_count = chromatogram_meta.numeric_values.len() as u32;
    let chrom_str_count = chromatogram_meta.string_offsets.len() as u32;

    let global_meta_count = global_meta.ref_codes.len() as u32;
    let global_num_count = global_meta.numeric_values.len() as u32;
    let global_str_count = global_meta.string_offsets.len() as u32;

    let mut spectrum_meta_bytes = write_packed_meta_bytes(&spectrum_meta);
    let mut chromatogram_meta_bytes = write_packed_meta_bytes(&chromatogram_meta);
    let mut global_meta_bytes = write_global_meta_bytes(&global_counts, &global_meta);

    let size_spec_meta_uncompressed = spectrum_meta_bytes.len() as u64;
    let size_chrom_meta_uncompressed = chromatogram_meta_bytes.len() as u64;
    let size_global_meta_uncompressed = global_meta_bytes.len() as u64;

    if compress_meta {
        spectrum_meta_bytes = compress_bytes(&spectrum_meta_bytes, compression_level);
        chromatogram_meta_bytes = compress_bytes(&chromatogram_meta_bytes, compression_level);
        global_meta_bytes = compress_bytes(&global_meta_bytes, compression_level);
    }

    let mut spec_entries_bytes = Vec::with_capacity(spectra.len() * 16);
    let mut spec_arrayrefs_bytes = Vec::new();

    let mut chrom_entries_bytes = Vec::with_capacity(chromatograms.len() * 16);
    let mut chrom_arrayrefs_bytes = Vec::new();

    let mut spect_array_types: HashSet<u32> = HashSet::new();
    let mut chrom_array_types: HashSet<u32> = HashSet::new();

    let mut spect_builder =
        ContainerBuilder::new(TARGET_BLOCK_UNCOMP_BYTES, compression_level, do_shuffle);
    let mut chrom_builder =
        ContainerBuilder::new(TARGET_BLOCK_UNCOMP_BYTES, compression_level, do_shuffle);

    let mut spec_a1_index: u64 = 0;

    for s in spectra {
        let arr_ref_start = spec_a1_index;
        let mut arr_ref_count: u64 = 0;

        if let Some(bal) = s.binary_data_array_list.as_ref() {
            for ba in &bal.binary_data_arrays {
                let arr = match array_ref(ba) {
                    Some(a) => a,
                    None => continue,
                };

                if arr.len() == 0 {
                    continue;
                }

                let array_type = array_type_tail(ba);
                if array_type != 0 {
                    spect_array_types.insert(array_type);
                }

                let writer_dtype = dtype_of(ba, arr, f32_compress);
                let entry_dtype = entry_dtype_from_writer_dtype(writer_dtype);

                let esz = dtype_elem_size(writer_dtype);
                let item_bytes = arr.len() * esz;

                let (block_id, element_off) =
                    spect_builder.add_item_to_box(item_bytes, esz, |buf| {
                        write_array(buf, arr, writer_dtype)
                    });

                let len_elements = arr.len() as u64;

                write_u64_le(&mut spec_arrayrefs_bytes, element_off);
                write_u64_le(&mut spec_arrayrefs_bytes, len_elements);
                write_u32_le(&mut spec_arrayrefs_bytes, block_id);
                write_u32_le(&mut spec_arrayrefs_bytes, array_type);
                spec_arrayrefs_bytes.push(entry_dtype);
                spec_arrayrefs_bytes.extend_from_slice(&[0u8; 7]);

                spec_a1_index += 1;
                arr_ref_count += 1;
            }
        }

        write_u64_le(&mut spec_entries_bytes, arr_ref_start);
        write_u64_le(&mut spec_entries_bytes, arr_ref_count);
    }

    let mut chrom_b1_index: u64 = 0;

    for c in chromatograms {
        let arr_ref_start = chrom_b1_index;
        let mut arr_ref_count: u64 = 0;

        if let Some(bal) = c.binary_data_array_list.as_ref() {
            for ba in &bal.binary_data_arrays {
                let arr = match array_ref(ba) {
                    Some(a) => a,
                    None => continue,
                };

                if arr.len() == 0 {
                    continue;
                }

                let array_type = array_type_tail(ba);
                if array_type != 0 {
                    chrom_array_types.insert(array_type);
                }

                let writer_dtype = dtype_of(ba, arr, f32_compress);
                let entry_dtype = entry_dtype_from_writer_dtype(writer_dtype);

                let esz = dtype_elem_size(writer_dtype);
                let item_bytes = arr.len() * esz;

                let (block_id, element_off) =
                    chrom_builder.add_item_to_box(item_bytes, esz, |buf| {
                        write_array(buf, arr, writer_dtype)
                    });

                let len_elements = arr.len() as u64;

                write_u64_le(&mut chrom_arrayrefs_bytes, element_off);
                write_u64_le(&mut chrom_arrayrefs_bytes, len_elements);
                write_u32_le(&mut chrom_arrayrefs_bytes, block_id);
                write_u32_le(&mut chrom_arrayrefs_bytes, array_type);
                chrom_arrayrefs_bytes.push(entry_dtype);
                chrom_arrayrefs_bytes.extend_from_slice(&[0u8; 7]);

                chrom_b1_index += 1;
                arr_ref_count += 1;
            }
        }

        write_u64_le(&mut chrom_entries_bytes, arr_ref_start);
        write_u64_le(&mut chrom_entries_bytes, arr_ref_count);
    }

    let (container_spect, block_count_spect) = spect_builder.pack();
    let (container_chrom, block_count_chrom) = chrom_builder.pack();

    let mut output = Vec::with_capacity(
        HEADER_SIZE
            + spec_entries_bytes.len()
            + spec_arrayrefs_bytes.len()
            + chrom_entries_bytes.len()
            + chrom_arrayrefs_bytes.len()
            + spectrum_meta_bytes.len()
            + chromatogram_meta_bytes.len()
            + global_meta_bytes.len()
            + container_spect.len()
            + container_chrom.len()
            + 64,
    );

    output.resize(HEADER_SIZE, 0);

    let off_spec_entries = append_aligned_8(&mut output, &spec_entries_bytes);
    let len_spec_entries = spec_entries_bytes.len() as u64;

    let off_spec_arrayrefs = append_aligned_8(&mut output, &spec_arrayrefs_bytes);
    let len_spec_arrayrefs = spec_arrayrefs_bytes.len() as u64;

    let off_chrom_entries = append_aligned_8(&mut output, &chrom_entries_bytes);
    let len_chrom_entries = chrom_entries_bytes.len() as u64;

    let off_chrom_arrayrefs = append_aligned_8(&mut output, &chrom_arrayrefs_bytes);
    let len_chrom_arrayrefs = chrom_arrayrefs_bytes.len() as u64;

    let off_spec_meta = append_aligned_8(&mut output, &spectrum_meta_bytes);
    let len_spec_meta = spectrum_meta_bytes.len() as u64;

    let off_chrom_meta = append_aligned_8(&mut output, &chromatogram_meta_bytes);
    let len_chrom_meta = chromatogram_meta_bytes.len() as u64;

    let off_global_meta = append_aligned_8(&mut output, &global_meta_bytes);
    let len_global_meta = global_meta_bytes.len() as u64;

    let off_container_spect = append_aligned_8(&mut output, &container_spect);
    let len_container_spect = container_spect.len() as u64;

    let off_container_chrom = append_aligned_8(&mut output, &container_chrom);
    let len_container_chrom = container_chrom.len() as u64;

    {
        let header = &mut output[0..HEADER_SIZE];

        header[0..4].copy_from_slice(b"B000");
        set_u8_at(header, 4, 0);

        set_u64_at(header, 8, off_spec_entries);
        set_u64_at(header, 16, len_spec_entries);

        set_u64_at(header, 24, off_spec_arrayrefs);
        set_u64_at(header, 32, len_spec_arrayrefs);

        set_u64_at(header, 40, off_chrom_entries);
        set_u64_at(header, 48, len_chrom_entries);

        set_u64_at(header, 56, off_chrom_arrayrefs);
        set_u64_at(header, 64, len_chrom_arrayrefs);

        set_u64_at(header, 72, off_spec_meta);
        set_u64_at(header, 80, len_spec_meta);

        set_u64_at(header, 88, off_chrom_meta);
        set_u64_at(header, 96, len_chrom_meta);

        set_u64_at(header, 104, off_global_meta);
        set_u64_at(header, 112, len_global_meta);

        set_u64_at(header, 120, off_container_spect);
        set_u64_at(header, 128, len_container_spect);

        set_u64_at(header, 136, off_container_chrom);
        set_u64_at(header, 144, len_container_chrom);

        set_u32_at(header, 152, block_count_spect);
        set_u32_at(header, 156, block_count_chrom);

        set_u32_at(header, 160, spectrum_count);
        set_u32_at(header, 164, chrom_count);

        set_u32_at(header, 168, spec_meta_count);
        set_u32_at(header, 172, spec_num_count);
        set_u32_at(header, 176, spec_str_count);

        set_u32_at(header, 180, chrom_meta_count);
        set_u32_at(header, 184, chrom_num_count);
        set_u32_at(header, 188, chrom_str_count);

        set_u32_at(header, 192, global_meta_count);
        set_u32_at(header, 196, global_num_count);
        set_u32_at(header, 200, global_str_count);

        set_u32_at(header, 204, spect_array_types.len() as u32);
        set_u32_at(header, 208, chrom_array_types.len() as u32);

        set_u64_at(header, 216, TARGET_BLOCK_UNCOMP_BYTES as u64);

        set_u8_at(header, 224, compression_codec);
        set_u8_at(header, 225, compression_level);
        set_u8_at(header, 226, array_filter_id);

        set_u64_at(header, 232, size_spec_meta_uncompressed);
        set_u64_at(header, 240, size_chrom_meta_uncompressed);
        set_u64_at(header, 248, size_global_meta_uncompressed);
    }

    output.extend_from_slice(&FILE_TRAILER);
    output
}
