use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    slice,
};
use zstd::bulk::compress as zstd_compress;

use crate::{
    BinaryData, NumericType, UserParam,
    b64::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_CV_FULL_NAME, ACC_ATTR_CV_URI, ACC_ATTR_CV_VERSION,
            ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_DEFAULT_SOURCE_FILE_REF,
            ACC_ATTR_ID, ACC_ATTR_INDEX, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_LABEL,
            ACC_ATTR_LOCATION, ACC_ATTR_NAME, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SAMPLE_REF,
            ACC_ATTR_START_TIME_STAMP, ACC_ATTR_VERSION, CV_REF_ATTR, attr_cv_param,
            cv_ref_code_from_str,
        },
        utilities::{
            assign_attributes,
            container_builder::{CompressionMode, ContainerBuilder, DefaultCompressor, FilterType},
        },
    },
    decoder::decode::MetadatumValue,
    mzml::{
        schema::TagId,
        structs::{
            BinaryDataArray, Chromatogram, CvParam, MzML, Precursor, Product,
            ReferenceableParamGroup, ReferenceableParamGroupRef, ScanList, Spectrum,
        },
    },
};

pub(crate) const HEADER_SIZE: usize = 512;
pub(crate) const FILE_TRAILER: [u8; 8] = *b"END\0\0\0\0\0";
pub(crate) const TARGET_BLOCK_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

const ACCESSION_MZ_ARRAY: u32 = 1_000_514;
const ACCESSION_INTENSITY_ARRAY: u32 = 1_000_515;
const ACCESSION_TIME_ARRAY: u32 = 1_000_595;
const ACCESSION_32BIT_FLOAT: u32 = 1_000_521;
const ACCESSION_64BIT_FLOAT: u32 = 1_000_523;

const ARRAY_FILTER_NONE: u8 = 0;
const ARRAY_FILTER_BYTE_SHUFFLE: u8 = 1;

const DTYPE_F32: u8 = 1;
const DTYPE_F64: u8 = 2;
const DTYPE_F16: u8 = 3;
const DTYPE_I16: u8 = 4;
const DTYPE_I32: u8 = 5;
const DTYPE_I64: u8 = 6;

#[derive(Debug)]
pub(crate) struct MetadataBlock {
    pub(crate) index_offsets: Vec<u32>,
    pub(crate) ids: Vec<u32>,
    pub(crate) parent_indices: Vec<u32>,
    pub(crate) tag_ids: Vec<u8>,
    pub(crate) ref_codes: Vec<u8>,
    pub(crate) accession_numbers: Vec<u32>,
    pub(crate) unit_ref_codes: Vec<u8>,
    pub(crate) unit_accession_numbers: Vec<u32>,
    pub(crate) value_kinds: Vec<u8>,
    pub(crate) value_indices: Vec<u32>,
    pub(crate) numeric_values: Vec<f64>,
    pub(crate) string_offsets: Vec<u32>,
    pub(crate) string_lengths: Vec<u32>,
    pub(crate) string_bytes: Vec<u8>,
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
    cv_params: Vec<CvParam>,
    tag_ids: Vec<u8>,
    owner_ids: Vec<u32>,
    parent_ids: Vec<u32>,
}

#[derive(Debug, Default)]
struct NodeIdAllocator {
    next_id: u32,
}

impl NodeIdAllocator {
    fn new() -> Self {
        Self { next_id: 1 }
    }
    fn next(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[derive(Debug, Default)]
struct RowIdAllocator {
    next_id: u32,
}

impl RowIdAllocator {
    fn new() -> Self {
        Self { next_id: 1 }
    }
    fn next(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[derive(Copy, Clone)]
enum ArrayData<'a> {
    F16(&'a [u16]),
    F32(&'a [f32]),
    F64(&'a [f64]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
}

impl<'a> ArrayData<'a> {
    fn len(self) -> usize {
        match self {
            ArrayData::F16(slice) => slice.len(),
            ArrayData::F32(slice) => slice.len(),
            ArrayData::F64(slice) => slice.len(),
            ArrayData::I16(slice) => slice.len(),
            ArrayData::I32(slice) => slice.len(),
            ArrayData::I64(slice) => slice.len(),
        }
    }

    fn is_empty(self) -> bool {
        self.len() == 0
    }
}

struct MetadataCollector<'a> {
    cv_params: &'a mut Vec<CvParam>,
    tag_ids: &'a mut Vec<u8>,
    owner_ids: &'a mut Vec<u32>,
    parent_ids: &'a mut Vec<u32>,
}

impl<'a> MetadataCollector<'a> {
    fn new(
        cv_params: &'a mut Vec<CvParam>,
        tag_ids: &'a mut Vec<u8>,
        owner_ids: &'a mut Vec<u32>,
        parent_ids: &'a mut Vec<u32>,
    ) -> Self {
        Self {
            cv_params,
            tag_ids,
            owner_ids,
            parent_ids,
        }
    }

    fn push_one(&mut self, tag: TagId, owner: u32, parent: u32, cv: CvParam) {
        self.cv_params.push(cv);
        self.tag_ids.push(tag as u8);
        self.owner_ids.push(owner);
        self.parent_ids.push(parent);
    }

    fn push_many(&mut self, tag: TagId, owner: u32, parent: u32, cvs: &[CvParam]) {
        if cvs.is_empty() {
            return;
        }
        self.cv_params.extend_from_slice(cvs);
        let new_len = self.tag_ids.len() + cvs.len();
        self.tag_ids.resize(new_len, tag as u8);
        self.owner_ids.resize(new_len, owner);
        self.parent_ids.resize(new_len, parent);
    }

    fn push_user_params(&mut self, tag: TagId, owner: u32, parent: u32, user_params: &[UserParam]) {
        if user_params.is_empty() {
            return;
        }
        let new_len = self.tag_ids.len() + user_params.len();
        self.tag_ids.resize(new_len, tag as u8);
        self.owner_ids.resize(new_len, owner);
        self.parent_ids.resize(new_len, parent);
        self.cv_params.reserve(user_params.len());
        for user_param in user_params {
            self.cv_params.push(user_param_as_cv(user_param));
        }
    }

    fn touch(&mut self, tag: TagId, owner: u32, parent: u32) {
        self.push_one(tag, owner, parent, empty_cv());
    }

    fn push_str_attr(
        &mut self,
        tag: TagId,
        owner: u32,
        parent: u32,
        accession_tail: u32,
        value: &str,
    ) {
        if !value.is_empty() {
            self.push_one(tag, owner, parent, make_attr_cv(accession_tail, value));
        }
    }

    fn push_optional_u32_attr(
        &mut self,
        tag: TagId,
        owner: u32,
        parent: u32,
        accession_tail: u32,
        value: Option<u32>,
    ) {
        if let Some(v) = value {
            self.push_one(
                tag,
                owner,
                parent,
                make_attr_cv(accession_tail, &v.to_string()),
            );
        }
    }

    fn push_cv_and_user_params(
        &mut self,
        owner: u32,
        parent: u32,
        cvs: &[CvParam],
        user_params: &[UserParam],
    ) {
        self.push_many(TagId::CvParam, owner, parent, cvs);
        self.push_user_params(TagId::UserParam, owner, parent, user_params);
    }

    fn push_ref_group_params(
        &mut self,
        owner: u32,
        parent: u32,
        refs: &[ReferenceableParamGroupRef],
        ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    ) {
        for group_ref in refs {
            if let Some(group) = ref_groups.get(group_ref.r#ref.as_str()) {
                self.push_many(TagId::CvParam, owner, parent, &group.cv_params);
            }
        }
    }

    fn push_schema_attrs<T: Serialize>(&mut self, tag: TagId, owner: u32, parent: u32, value: &T) {
        for attr in assign_attributes(value, tag, owner, parent) {
            let accession_tail = parse_accession_tail(attr.accession.as_deref());
            if accession_tail == 0 {
                continue;
            }
            let text = match attr.value {
                MetadatumValue::Text(v) => v,
                MetadatumValue::Number(n) => n.to_string(),
                MetadatumValue::Empty => continue,
            };
            if text.is_empty() {
                continue;
            }
            self.push_one(tag, owner, parent, make_attr_cv(accession_tail, &text));
        }
    }
}

fn empty_cv() -> CvParam {
    CvParam {
        cv_ref: None,
        accession: None,
        name: String::new(),
        value: None,
        unit_cv_ref: None,
        unit_name: None,
        unit_accession: None,
    }
}

fn user_param_as_cv(user_param: &UserParam) -> CvParam {
    let encoded_value = match &user_param.value {
        Some(v) => format!("{}\0{}", user_param.name, v),
        None => format!("{}\0", user_param.name),
    };
    CvParam {
        cv_ref: None,
        accession: None,
        name: String::new(),
        value: Some(encoded_value),
        unit_cv_ref: user_param.unit_cv_ref.clone(),
        unit_name: user_param.unit_name.clone(),
        unit_accession: user_param.unit_accession.clone(),
    }
}

fn make_float_precision_cv(accession_tail: u32) -> CvParam {
    let name = if accession_tail == ACCESSION_32BIT_FLOAT {
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

fn make_attr_cv(accession_tail: u32, value: &str) -> CvParam {
    attr_cv_param(accession_tail, value)
}

fn array_data_from_binary_data_array(bda: &BinaryDataArray) -> Option<ArrayData<'_>> {
    let binary = bda.binary.as_ref()?;
    match binary {
        BinaryData::F16(v) => Some(ArrayData::F16(v.as_slice())),
        BinaryData::I16(v) => Some(ArrayData::I16(v.as_slice())),
        BinaryData::I32(v) => Some(ArrayData::I32(v.as_slice())),
        BinaryData::I64(v) => Some(ArrayData::I64(v.as_slice())),
        BinaryData::F32(v) => Some(ArrayData::F32(v.as_slice())),
        BinaryData::F64(v) => Some(ArrayData::F64(v.as_slice())),
    }
}

fn array_type_accession_from_bda(bda: &BinaryDataArray) -> u32 {
    for cv in &bda.cv_params {
        let tail = parse_accession_tail(cv.accession.as_deref());
        if matches!(tail, _ if tail == ACCESSION_MZ_ARRAY || tail == ACCESSION_INTENSITY_ARRAY || tail == ACCESSION_TIME_ARRAY)
        {
            return tail;
        }
    }
    for cv in &bda.cv_params {
        let tail = parse_accession_tail(cv.accession.as_deref());
        if tail != 0 && cv.name.to_ascii_lowercase().contains(" array") {
            return tail;
        }
    }
    0
}

fn element_size_for_dtype(dtype: u8) -> usize {
    match dtype {
        DTYPE_F16 | DTYPE_I16 => 2,
        DTYPE_F32 | DTYPE_I32 => 4,
        DTYPE_F64 | DTYPE_I64 => 8,
        _ => 1,
    }
}

fn resolve_write_dtype(bda: &BinaryDataArray, data: ArrayData<'_>, force_f32: bool) -> u8 {
    match data {
        ArrayData::F16(_) => DTYPE_F16,
        ArrayData::I16(_) => DTYPE_I16,
        ArrayData::I32(_) => DTYPE_I32,
        ArrayData::I64(_) => DTYPE_I64,
        ArrayData::F32(_) | ArrayData::F64(_) => {
            if should_use_f64(bda, data, force_f32) {
                DTYPE_F64
            } else {
                DTYPE_F32
            }
        }
    }
}

fn should_use_f64(bda: &BinaryDataArray, data: ArrayData<'_>, force_f32: bool) -> bool {
    if force_f32 {
        return false;
    }
    declared_float_precision_is_64bit(bda).unwrap_or(matches!(data, ArrayData::F64(_)))
}

fn declared_float_precision_is_64bit(bda: &BinaryDataArray) -> Option<bool> {
    if let Some(numeric_type) = bda.numeric_type.as_ref() {
        return match numeric_type {
            NumericType::Float64 => Some(true),
            NumericType::Float32 => Some(false),
            _ => None,
        };
    }
    let (mut saw_f32, mut saw_f64) = (false, false);
    for cv in &bda.cv_params {
        match parse_accession_tail(cv.accession.as_deref()) {
            ACCESSION_32BIT_FLOAT => saw_f32 = true,
            ACCESSION_64BIT_FLOAT => saw_f64 = true,
            _ => {}
        }
        if saw_f32 && saw_f64 {
            break;
        }
    }
    match (saw_f32, saw_f64) {
        (true, false) => Some(false),
        (false, true) => Some(true),
        _ => None,
    }
}

fn writer_dtype_to_entry_dtype(writer_dtype: u8) -> u8 {
    match writer_dtype {
        1 => 2,
        2 => 1,
        _ => writer_dtype,
    }
}

fn compress_if_needed(data: &[u8], compression_level: u8) -> Vec<u8> {
    if compression_level == 0 {
        return data.to_vec();
    }
    zstd_compress(data, compression_level as i32).expect("zstd compression failed")
}

fn write_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}
fn write_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}
fn write_f64_le(buf: &mut Vec<u8>, value: f64) {
    buf.extend_from_slice(&value.to_le_bytes());
}
fn write_f32_le(buf: &mut Vec<u8>, value: f32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn patch_u8_at(buf: &mut [u8], offset: usize, value: u8) {
    buf[offset] = value;
}
fn patch_u32_at(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
fn patch_u64_at(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

macro_rules! write_typed_slice_le {
    ($fn_name:ident, $elem_ty:ty, $elem_size:expr, $scalar_writer:ident) => {
        fn $fn_name(buf: &mut Vec<u8>, values: &[$elem_ty]) {
            if cfg!(target_endian = "little") {
                unsafe {
                    buf.extend_from_slice(slice::from_raw_parts(
                        values.as_ptr() as *const u8,
                        values.len() * $elem_size,
                    ));
                }
            } else {
                for &v in values {
                    $scalar_writer(buf, v);
                }
            }
        }
    };
}

write_typed_slice_le!(write_u16_slice_le, u16, 2, write_u16_le_scalar);
write_typed_slice_le!(write_i16_slice_le, i16, 2, write_i16_le_scalar);
write_typed_slice_le!(write_i32_slice_le, i32, 4, write_i32_le_scalar);
write_typed_slice_le!(write_i64_slice_le, i64, 8, write_i64_le_scalar);
write_typed_slice_le!(write_u32_slice_le, u32, 4, write_u32_le);
write_typed_slice_le!(write_f32_slice_le, f32, 4, write_f32_le);
write_typed_slice_le!(write_f64_slice_le, f64, 8, write_f64_le);

fn write_u16_le_scalar(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_i16_le_scalar(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_i32_le_scalar(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_i64_le_scalar(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_array_data(buf: &mut Vec<u8>, data: ArrayData<'_>, dtype: u8) {
    match (dtype, data) {
        (DTYPE_F16, ArrayData::F16(xs)) => write_u16_slice_le(buf, xs),
        (DTYPE_F32, ArrayData::F32(xs)) => write_f32_slice_le(buf, xs),
        (DTYPE_F32, ArrayData::F64(xs)) => {
            for &v in xs {
                write_f32_le(buf, v as f32);
            }
        }
        (DTYPE_F64, ArrayData::F64(xs)) => write_f64_slice_le(buf, xs),
        (DTYPE_F64, ArrayData::F32(xs)) => {
            for &v in xs {
                write_f64_le(buf, v as f64);
            }
        }
        (DTYPE_I16, ArrayData::I16(xs)) => write_i16_slice_le(buf, xs),
        (DTYPE_I32, ArrayData::I32(xs)) => write_i32_slice_le(buf, xs),
        (DTYPE_I64, ArrayData::I64(xs)) => write_i64_slice_le(buf, xs),
        _ => {}
    }
}

fn parse_accession_tail(accession: Option<&str>) -> u32 {
    let tail = accession
        .unwrap_or("")
        .rsplit_once(':')
        .map(|(_, tail)| tail)
        .unwrap_or("");

    let mut value: u32 = 0;
    let mut saw_digit = false;
    for byte in tail.bytes() {
        if (b'0'..=b'9').contains(&byte) {
            saw_digit = true;
            let digit = (byte - b'0') as u32;
            value = match value.checked_mul(10).and_then(|x| x.checked_add(digit)) {
                Some(n) => n,
                None => return 0,
            };
        }
    }
    if saw_digit { value } else { 0 }
}

fn cv_ref_prefix_from_accession(accession: Option<&str>) -> Option<&str> {
    accession.and_then(|s| s.split_once(':').map(|(prefix, _)| prefix))
}

fn align_offset_to_8_bytes(offset: usize) -> usize {
    (offset + 7) & !7
}

fn append_8byte_aligned(output: &mut Vec<u8>, bytes: &[u8]) -> u64 {
    let aligned_offset = align_offset_to_8_bytes(output.len());
    if aligned_offset > output.len() {
        output.resize(aligned_offset, 0);
    }
    let final_offset = output.len() as u64;
    output.extend_from_slice(bytes);
    final_offset
}

fn normalize_attr_cv_values(cv_params: &mut Vec<CvParam>) {
    for cv in cv_params.iter_mut() {
        if cv.cv_ref.as_deref() == Some(CV_REF_ATTR) {
            let value_is_empty = cv.value.as_deref().map_or(true, |s| s.is_empty());
            if value_is_empty && !cv.name.is_empty() {
                cv.value = Some(std::mem::take(&mut cv.name));
            }
        }
    }
}

fn build_ref_group_lookup<'a>(mzml: &'a MzML) -> HashMap<&'a str, &'a ReferenceableParamGroup> {
    match &mzml.referenceable_param_group_list {
        None => HashMap::new(),
        Some(list) => list
            .referenceable_param_groups
            .iter()
            .map(|group| (group.id.as_str(), group))
            .collect(),
    }
}

fn emit_bda_cvparams_with_float_precision_override(
    sink: &mut MetadataCollector<'_>,
    bda_node_id: u32,
    bda_list_node_id: u32,
    bda: &BinaryDataArray,
    x_array_accession: u32,
    y_array_accession: u32,
    force_f32: bool,
) {
    let bda_is_x_or_y_array = bda.cv_params.iter().any(|cv| {
        let tail = parse_accession_tail(cv.accession.as_deref());
        tail == x_array_accession || tail == y_array_accession
    });

    if !(force_f32 && bda_is_x_or_y_array) {
        sink.push_many(
            TagId::CvParam,
            bda_node_id,
            bda_list_node_id,
            &bda.cv_params,
        );
        return;
    }

    let mut float_precision_cv_written = false;
    for cv in &bda.cv_params {
        let tail = parse_accession_tail(cv.accession.as_deref());
        if tail == ACCESSION_32BIT_FLOAT || tail == ACCESSION_64BIT_FLOAT {
            if !float_precision_cv_written {
                sink.push_one(
                    TagId::CvParam,
                    bda_node_id,
                    bda_list_node_id,
                    make_float_precision_cv(ACCESSION_32BIT_FLOAT),
                );
                float_precision_cv_written = true;
            }
        } else {
            sink.push_one(TagId::CvParam, bda_node_id, bda_list_node_id, cv.clone());
        }
    }
    if !float_precision_cv_written {
        sink.push_one(
            TagId::CvParam,
            bda_node_id,
            bda_list_node_id,
            make_float_precision_cv(ACCESSION_32BIT_FLOAT),
        );
    }
}

fn packed_meta_byte_size(meta: &MetadataBlock) -> usize {
    meta.index_offsets.len() * 4
        + meta.ids.len() * 4
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

fn write_packed_meta_into_buffer(buf: &mut Vec<u8>, meta: &MetadataBlock) {
    write_u32_slice_le(buf, &meta.index_offsets);
    write_u32_slice_le(buf, &meta.ids);
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

fn serialize_packed_meta(meta: &MetadataBlock) -> Vec<u8> {
    let mut buf = Vec::with_capacity(packed_meta_byte_size(meta));
    write_packed_meta_into_buffer(&mut buf, meta);
    buf
}

fn serialize_global_meta_with_counts(counts: &GlobalCounts, meta: &MetadataBlock) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 * 4 + packed_meta_byte_size(meta));
    write_u32_le(&mut buf, counts.n_file_description);
    write_u32_le(&mut buf, counts.n_run);
    write_u32_le(&mut buf, counts.n_ref_param_groups);
    write_u32_le(&mut buf, counts.n_samples);
    write_u32_le(&mut buf, counts.n_instrument_configs);
    write_u32_le(&mut buf, counts.n_software);
    write_u32_le(&mut buf, counts.n_data_processing);
    write_u32_le(&mut buf, counts.n_acquisition_settings);
    write_u32_le(&mut buf, counts.n_cvs);
    write_packed_meta_into_buffer(&mut buf, meta);
    buf
}

fn encode_cv_value(
    value: Option<&str>,
    numeric_values: &mut Vec<f64>,
    string_offsets: &mut Vec<u32>,
    string_lengths: &mut Vec<u32>,
    string_bytes: &mut Vec<u8>,
    numeric_count: &mut u32,
    string_count: &mut u32,
) -> (u8, u32) {
    match value {
        None | Some("") => (2u8, 0u32),
        Some(text) => {
            if let Ok(number) = text.parse::<f64>() {
                let index = *numeric_count;
                numeric_values.push(number);
                *numeric_count += 1;
                (0u8, index)
            } else {
                let index = *string_count;
                let bytes = text.as_bytes();
                string_offsets.push(string_bytes.len() as u32);
                string_lengths.push(bytes.len() as u32);
                string_bytes.extend_from_slice(bytes);
                *string_count += 1;
                (1u8, index)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn pack_single_cv_row(
    tag_id: u8,
    owner_id: u32,
    parent_id: u32,
    cv: &CvParam,
    tag_ids: &mut Vec<u8>,
    ids: &mut Vec<u32>,
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
    numeric_count: &mut u32,
    string_count: &mut u32,
) {
    tag_ids.push(tag_id);
    ids.push(owner_id);
    parent_indices.push(parent_id);

    let cv_ref = cv_ref_prefix_from_accession(cv.accession.as_deref()).or(cv.cv_ref.as_deref());
    ref_codes.push(cv_ref_code_from_str(cv_ref));
    accession_numbers.push(parse_accession_tail(cv.accession.as_deref()));

    let unit_ref =
        cv_ref_prefix_from_accession(cv.unit_accession.as_deref()).or(cv.unit_cv_ref.as_deref());
    unit_ref_codes.push(cv_ref_code_from_str(unit_ref));
    unit_accession_numbers.push(parse_accession_tail(cv.unit_accession.as_deref()));

    let (kind, index) = encode_cv_value(
        cv.value.as_deref(),
        numeric_values,
        string_offsets,
        string_lengths,
        string_bytes,
        numeric_count,
        string_count,
    );
    value_kinds.push(kind);
    value_indices.push(index);
}

fn build_metadata_block<T, F>(
    row_ids: &mut RowIdAllocator,
    items: &[T],
    mut fill: F,
) -> MetadataBlock
where
    F: FnMut(&mut MetadataCollector<'_>, &T),
{
    let mut index_offsets = Vec::with_capacity(items.len() + 1);
    let mut ids: Vec<u32> = Vec::new();
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

    let mut scratch_cvs: Vec<CvParam> = Vec::with_capacity(256);
    let mut scratch_tags: Vec<u8> = Vec::with_capacity(256);
    let mut scratch_owners: Vec<u32> = Vec::with_capacity(256);
    let mut scratch_parents: Vec<u32> = Vec::with_capacity(256);

    let mut numeric_count: u32 = 0;
    let mut string_count: u32 = 0;
    let mut row_count: u32 = 0;

    index_offsets.push(0);

    for item in items {
        scratch_cvs.clear();
        scratch_tags.clear();
        scratch_owners.clear();
        scratch_parents.clear();

        {
            let mut sink = MetadataCollector::new(
                &mut scratch_cvs,
                &mut scratch_tags,
                &mut scratch_owners,
                &mut scratch_parents,
            );
            fill(&mut sink, item);
        }

        debug_assert_eq!(scratch_cvs.len(), scratch_tags.len());
        debug_assert_eq!(scratch_cvs.len(), scratch_owners.len());
        debug_assert_eq!(scratch_cvs.len(), scratch_parents.len());

        for i in 0..scratch_cvs.len() {
            let _ = row_ids.next();
            pack_single_cv_row(
                scratch_tags[i],
                scratch_owners[i],
                scratch_parents[i],
                &scratch_cvs[i],
                &mut tag_ids,
                &mut ids,
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
                &mut numeric_count,
                &mut string_count,
            );
            row_count += 1;
        }

        index_offsets.push(row_count);
    }

    MetadataBlock {
        index_offsets,
        ids,
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

fn pack_meta_from_slices<T, F>(
    row_ids: &mut RowIdAllocator,
    items: &[T],
    row_data_of: F,
) -> MetadataBlock
where
    F: Fn(&T) -> (&[CvParam], &[u8], &[u32], &[u32]),
{
    let total_rows: usize = items.iter().map(|item| row_data_of(item).0.len()).sum();

    let mut index_offsets = Vec::with_capacity(items.len() + 1);
    let mut ids = Vec::with_capacity(total_rows);
    let mut parent_indices = Vec::with_capacity(total_rows);
    let mut tag_ids = Vec::with_capacity(total_rows);
    let mut ref_codes = Vec::with_capacity(total_rows);
    let mut accession_numbers = Vec::with_capacity(total_rows);
    let mut unit_ref_codes = Vec::with_capacity(total_rows);
    let mut unit_accession_numbers = Vec::with_capacity(total_rows);
    let mut value_kinds = Vec::with_capacity(total_rows);
    let mut value_indices = Vec::with_capacity(total_rows);
    let mut numeric_values = Vec::with_capacity(total_rows);
    let mut string_offsets = Vec::with_capacity(total_rows);
    let mut string_lengths = Vec::with_capacity(total_rows);
    let mut string_bytes = Vec::new();

    let mut numeric_count: u32 = 0;
    let mut string_count: u32 = 0;
    let mut row_count: u32 = 0;

    index_offsets.push(0);

    for item in items {
        let (cvs, tags, owners, parents) = row_data_of(item);
        debug_assert_eq!(cvs.len(), tags.len());
        debug_assert_eq!(cvs.len(), owners.len());
        debug_assert_eq!(cvs.len(), parents.len());

        for i in 0..cvs.len() {
            let _ = row_ids.next();
            pack_single_cv_row(
                tags[i],
                owners[i],
                parents[i],
                &cvs[i],
                &mut tag_ids,
                &mut ids,
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
                &mut numeric_count,
                &mut string_count,
            );
            row_count += 1;
        }

        index_offsets.push(row_count);
    }

    MetadataBlock {
        index_offsets,
        ids,
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

fn build_global_meta_items(
    mzml: &MzML,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    node_ids: &mut NodeIdAllocator,
) -> (Vec<GlobalMetaItem>, GlobalCounts) {
    let mut items: Vec<GlobalMetaItem> = Vec::new();

    build_file_description_meta(mzml, ref_group_lookup, node_ids, &mut items);
    build_run_meta(mzml, node_ids, &mut items);

    let ref_groups_start = items.len();
    build_referenceable_param_groups_meta(mzml, node_ids, &mut items);
    let n_ref_param_groups = (items.len() - ref_groups_start) as u32;

    let samples_start = items.len();
    build_samples_meta(mzml, ref_group_lookup, node_ids, &mut items);
    let n_samples = (items.len() - samples_start) as u32;

    let instruments_start = items.len();
    build_instruments_meta(mzml, ref_group_lookup, node_ids, &mut items);
    let n_instrument_configs = (items.len() - instruments_start) as u32;

    let software_start = items.len();
    build_software_list_meta(mzml, node_ids, &mut items);
    let n_software = (items.len() - software_start) as u32;

    let data_processing_start = items.len();
    build_data_processing_list_meta(mzml, ref_group_lookup, node_ids, &mut items);
    let n_data_processing = (items.len() - data_processing_start) as u32;

    let scan_settings_start = items.len();
    build_scan_settings_list_meta(mzml, ref_group_lookup, node_ids, &mut items);
    let n_acquisition_settings = (items.len() - scan_settings_start) as u32;

    let cv_list_start = items.len();
    build_cv_list_meta(mzml, node_ids, &mut items);
    let n_cvs = (items.len() - cv_list_start) as u32;

    let counts = GlobalCounts {
        n_file_description: 1,
        n_run: 1,
        n_ref_param_groups,
        n_samples,
        n_instrument_configs,
        n_software,
        n_data_processing,
        n_acquisition_settings,
        n_cvs,
    };

    (items, counts)
}

fn new_global_meta_item() -> (Vec<CvParam>, Vec<u8>, Vec<u32>, Vec<u32>) {
    (Vec::new(), Vec::new(), Vec::new(), Vec::new())
}

fn commit_global_meta_item(
    items: &mut Vec<GlobalMetaItem>,
    cv_params: Vec<CvParam>,
    tag_ids: Vec<u8>,
    owner_ids: Vec<u32>,
    parent_ids: Vec<u32>,
) {
    items.push(GlobalMetaItem {
        cv_params,
        tag_ids,
        owner_ids,
        parent_ids,
    });
}

fn build_file_description_meta(
    mzml: &MzML,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(file_description) = &mzml.file_description else {
        return;
    };

    let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
    let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

    let file_description_id = node_ids.next();
    sink.touch(TagId::FileDescription, file_description_id, 0);

    let file_content_id = node_ids.next();
    sink.touch(TagId::FileContent, file_content_id, file_description_id);
    sink.push_ref_group_params(
        file_content_id,
        file_description_id,
        &file_description.file_content.referenceable_param_group_refs,
        ref_group_lookup,
    );
    sink.push_cv_and_user_params(
        file_content_id,
        file_description_id,
        &file_description.file_content.cv_params,
        &file_description.file_content.user_params,
    );

    let source_file_list_id = node_ids.next();
    sink.touch(
        TagId::SourceFileList,
        source_file_list_id,
        file_description_id,
    );
    sink.push_optional_u32_attr(
        TagId::SourceFileList,
        source_file_list_id,
        file_description_id,
        ACC_ATTR_COUNT,
        Some(file_description.source_file_list.source_file.len() as u32),
    );

    for source_file in &file_description.source_file_list.source_file {
        let source_file_id = node_ids.next();
        sink.touch(TagId::SourceFile, source_file_id, source_file_list_id);
        sink.push_str_attr(
            TagId::SourceFile,
            source_file_id,
            source_file_list_id,
            ACC_ATTR_ID,
            &source_file.id,
        );
        sink.push_str_attr(
            TagId::SourceFile,
            source_file_id,
            source_file_list_id,
            ACC_ATTR_NAME,
            &source_file.name,
        );
        sink.push_str_attr(
            TagId::SourceFile,
            source_file_id,
            source_file_list_id,
            ACC_ATTR_LOCATION,
            &source_file.location,
        );
        sink.push_ref_group_params(
            source_file_id,
            source_file_list_id,
            &source_file.referenceable_param_group_ref,
            ref_group_lookup,
        );
        sink.push_cv_and_user_params(
            source_file_id,
            source_file_list_id,
            &source_file.cv_param,
            &source_file.user_param,
        );
    }

    for contact in &file_description.contacts {
        let contact_id = node_ids.next();
        sink.touch(TagId::Contact, contact_id, file_description_id);
        sink.push_ref_group_params(
            contact_id,
            file_description_id,
            &contact.referenceable_param_group_refs,
            ref_group_lookup,
        );
        sink.push_cv_and_user_params(
            contact_id,
            file_description_id,
            &contact.cv_params,
            &contact.user_params,
        );
    }

    commit_global_meta_item(items, cvs, tags, owners, parents);
}

fn build_run_meta(mzml: &MzML, node_ids: &mut NodeIdAllocator, items: &mut Vec<GlobalMetaItem>) {
    let run = &mzml.run;
    let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
    let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

    let run_id = node_ids.next();
    sink.touch(TagId::Run, run_id, 0);
    sink.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_ID, &run.id);
    if let Some(ts) = run.start_time_stamp.as_deref() {
        sink.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_START_TIME_STAMP, ts);
    }
    if let Some(r) = run.default_instrument_configuration_ref.as_deref() {
        sink.push_str_attr(
            TagId::Run,
            run_id,
            0,
            ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
            r,
        );
    }
    if let Some(r) = run.default_source_file_ref.as_deref() {
        sink.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_DEFAULT_SOURCE_FILE_REF, r);
    }
    if let Some(r) = run.sample_ref.as_deref() {
        sink.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_SAMPLE_REF, r);
    }

    if let Some(source_file_ref_list) = &run.source_file_ref_list {
        let sfrl_id = node_ids.next();
        sink.touch(TagId::SourceFileRefList, sfrl_id, run_id);
        sink.push_optional_u32_attr(
            TagId::SourceFileRefList,
            sfrl_id,
            run_id,
            ACC_ATTR_COUNT,
            Some(source_file_ref_list.source_file_refs.len() as u32),
        );
        for sfr in &source_file_ref_list.source_file_refs {
            let sfr_id = node_ids.next();
            sink.touch(TagId::SourceFileRef, sfr_id, sfrl_id);
            sink.push_str_attr(
                TagId::SourceFileRef,
                sfr_id,
                sfrl_id,
                ACC_ATTR_REF,
                &sfr.r#ref,
            );
        }
    }

    for rgr in &run.referenceable_param_group_refs {
        let rgr_id = node_ids.next();
        sink.touch(TagId::ReferenceableParamGroupRef, rgr_id, run_id);
        sink.push_str_attr(
            TagId::ReferenceableParamGroupRef,
            rgr_id,
            run_id,
            ACC_ATTR_REF,
            &rgr.r#ref,
        );
    }

    sink.push_cv_and_user_params(run_id, 0, &run.cv_params, &run.user_params);

    commit_global_meta_item(items, cvs, tags, owners, parents);
}

fn build_referenceable_param_groups_meta(
    mzml: &MzML,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(list) = &mzml.referenceable_param_group_list else {
        return;
    };

    for group in &list.referenceable_param_groups {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        let group_id = node_ids.next();
        sink.touch(TagId::ReferenceableParamGroup, group_id, 0);
        sink.push_str_attr(
            TagId::ReferenceableParamGroup,
            group_id,
            0,
            ACC_ATTR_ID,
            &group.id,
        );
        sink.push_cv_and_user_params(group_id, 0, &group.cv_params, &group.user_params);

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn build_samples_meta(
    mzml: &MzML,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(sample_list) = &mzml.sample_list else {
        return;
    };

    for sample in &sample_list.samples {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        let sample_id = node_ids.next();
        sink.touch(TagId::Sample, sample_id, 0);
        sink.push_str_attr(TagId::Sample, sample_id, 0, ACC_ATTR_ID, &sample.id);
        sink.push_str_attr(TagId::Sample, sample_id, 0, ACC_ATTR_NAME, &sample.name);
        if let Some(group_ref) = &sample.referenceable_param_group_ref {
            sink.push_ref_group_params(sample_id, 0, slice::from_ref(group_ref), ref_group_lookup);
        }

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn build_instruments_meta(
    mzml: &MzML,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(instrument_list) = &mzml.instrument_list else {
        return;
    };

    for instrument in &instrument_list.instrument {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        let instrument_id = node_ids.next();
        sink.touch(TagId::Instrument, instrument_id, 0);
        sink.push_str_attr(
            TagId::Instrument,
            instrument_id,
            0,
            ACC_ATTR_ID,
            &instrument.id,
        );
        sink.push_ref_group_params(
            instrument_id,
            0,
            &instrument.referenceable_param_group_ref,
            ref_group_lookup,
        );
        sink.push_cv_and_user_params(
            instrument_id,
            0,
            &instrument.cv_param,
            &instrument.user_param,
        );

        if let Some(component_list) = &instrument.component_list {
            for source in &component_list.source {
                let source_id = node_ids.next();
                sink.touch(TagId::ComponentSource, source_id, instrument_id);
                sink.push_optional_u32_attr(
                    TagId::ComponentSource,
                    source_id,
                    instrument_id,
                    ACC_ATTR_ORDER,
                    source.order,
                );
                sink.push_ref_group_params(
                    source_id,
                    instrument_id,
                    &source.referenceable_param_group_ref,
                    ref_group_lookup,
                );
                sink.push_cv_and_user_params(
                    source_id,
                    instrument_id,
                    &source.cv_param,
                    &source.user_param,
                );
            }
            for analyzer in &component_list.analyzer {
                let analyzer_id = node_ids.next();
                sink.touch(TagId::ComponentAnalyzer, analyzer_id, instrument_id);
                sink.push_optional_u32_attr(
                    TagId::ComponentAnalyzer,
                    analyzer_id,
                    instrument_id,
                    ACC_ATTR_ORDER,
                    analyzer.order,
                );
                sink.push_ref_group_params(
                    analyzer_id,
                    instrument_id,
                    &analyzer.referenceable_param_group_ref,
                    ref_group_lookup,
                );
                sink.push_cv_and_user_params(
                    analyzer_id,
                    instrument_id,
                    &analyzer.cv_param,
                    &analyzer.user_param,
                );
            }
            for detector in &component_list.detector {
                let detector_id = node_ids.next();
                sink.touch(TagId::ComponentDetector, detector_id, instrument_id);
                sink.push_optional_u32_attr(
                    TagId::ComponentDetector,
                    detector_id,
                    instrument_id,
                    ACC_ATTR_ORDER,
                    detector.order,
                );
                sink.push_ref_group_params(
                    detector_id,
                    instrument_id,
                    &detector.referenceable_param_group_ref,
                    ref_group_lookup,
                );
                sink.push_cv_and_user_params(
                    detector_id,
                    instrument_id,
                    &detector.cv_param,
                    &detector.user_param,
                );
            }
        }

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn build_software_list_meta(
    mzml: &MzML,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(software_list) = &mzml.software_list else {
        return;
    };

    for software in &software_list.software {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        let software_id = node_ids.next();
        sink.touch(TagId::Software, software_id, 0);
        sink.push_str_attr(TagId::Software, software_id, 0, ACC_ATTR_ID, &software.id);

        let version = software.version.as_deref().or_else(|| {
            software
                .software_param
                .first()
                .and_then(|p| p.version.as_deref())
        });
        if let Some(v) = version {
            sink.push_str_attr(TagId::Software, software_id, 0, ACC_ATTR_VERSION, v);
        }

        for sw_param in &software.software_param {
            let sw_param_id = node_ids.next();
            sink.touch(TagId::SoftwareParam, sw_param_id, software_id);
            sink.push_one(
                TagId::SoftwareParam,
                sw_param_id,
                software_id,
                CvParam {
                    cv_ref: sw_param.cv_ref.clone(),
                    accession: Some(sw_param.accession.clone()),
                    name: sw_param.name.clone(),
                    value: Some(String::new()),
                    unit_cv_ref: None,
                    unit_name: None,
                    unit_accession: None,
                },
            );
        }

        sink.push_cv_and_user_params(software_id, 0, &software.cv_param, &software.user_params);

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn build_data_processing_list_meta(
    mzml: &MzML,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(dp_list) = &mzml.data_processing_list else {
        return;
    };

    for dp in &dp_list.data_processing {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        let dp_id = node_ids.next();
        sink.touch(TagId::DataProcessing, dp_id, 0);
        sink.push_str_attr(TagId::DataProcessing, dp_id, 0, ACC_ATTR_ID, &dp.id);

        for method in &dp.processing_method {
            let method_id = node_ids.next();
            sink.touch(TagId::ProcessingMethod, method_id, dp_id);
            sink.push_ref_group_params(
                method_id,
                dp_id,
                &method.referenceable_param_group_ref,
                ref_group_lookup,
            );
            sink.push_cv_and_user_params(method_id, dp_id, &method.cv_param, &method.user_param);
        }

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn build_scan_settings_list_meta(
    mzml: &MzML,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(settings_list) = &mzml.scan_settings_list else {
        return;
    };

    for settings in &settings_list.scan_settings {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        let settings_id = node_ids.next();
        sink.touch(TagId::ScanSettings, settings_id, 0);
        if let Some(id) = settings.id.as_deref() {
            sink.push_str_attr(TagId::ScanSettings, settings_id, 0, ACC_ATTR_ID, id);
        }
        if let Some(r) = settings.instrument_configuration_ref.as_deref() {
            sink.push_str_attr(
                TagId::ScanSettings,
                settings_id,
                0,
                ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
                r,
            );
        }

        if let Some(sfrl) = &settings.source_file_ref_list {
            let sfrl_id = node_ids.next();
            sink.touch(TagId::SourceFileRefList, sfrl_id, settings_id);
            sink.push_optional_u32_attr(
                TagId::SourceFileRefList,
                sfrl_id,
                settings_id,
                ACC_ATTR_COUNT,
                Some(sfrl.source_file_refs.len() as u32),
            );
            for sfr in &sfrl.source_file_refs {
                let sfr_id = node_ids.next();
                sink.touch(TagId::SourceFileRef, sfr_id, sfrl_id);
                sink.push_str_attr(
                    TagId::SourceFileRef,
                    sfr_id,
                    sfrl_id,
                    ACC_ATTR_REF,
                    &sfr.r#ref,
                );
            }
        }

        sink.push_ref_group_params(
            settings_id,
            0,
            &settings.referenceable_param_group_refs,
            ref_group_lookup,
        );
        sink.push_cv_and_user_params(settings_id, 0, &settings.cv_params, &settings.user_params);

        if let Some(target_list) = &settings.target_list {
            for target in &target_list.targets {
                let target_id = node_ids.next();
                sink.touch(TagId::Target, target_id, settings_id);
                sink.push_ref_group_params(
                    target_id,
                    settings_id,
                    &target.referenceable_param_group_refs,
                    ref_group_lookup,
                );
                sink.push_cv_and_user_params(
                    target_id,
                    settings_id,
                    &target.cv_params,
                    &target.user_params,
                );
            }
        }

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn build_cv_list_meta(
    mzml: &MzML,
    node_ids: &mut NodeIdAllocator,
    items: &mut Vec<GlobalMetaItem>,
) {
    let Some(cv_list) = &mzml.cv_list else { return };
    let cv_count = cv_list.cv.len() as u32;
    if cv_count == 0 {
        return;
    }

    let cv_list_node_id = node_ids.next();

    for (index, cv) in cv_list.cv.iter().enumerate() {
        let (mut cvs, mut tags, mut owners, mut parents) = new_global_meta_item();
        let mut sink = MetadataCollector::new(&mut cvs, &mut tags, &mut owners, &mut parents);

        if index == 0 {
            sink.touch(TagId::CvList, cv_list_node_id, 0);
            sink.push_optional_u32_attr(
                TagId::CvList,
                cv_list_node_id,
                0,
                ACC_ATTR_COUNT,
                Some(cv_count),
            );
        }

        let cv_node_id = node_ids.next();
        sink.touch(TagId::Cv, cv_node_id, cv_list_node_id);
        sink.push_str_attr(
            TagId::Cv,
            cv_node_id,
            cv_list_node_id,
            ACC_ATTR_LABEL,
            &cv.id,
        );
        if let Some(name) = cv.full_name.as_deref().filter(|s| !s.is_empty()) {
            sink.push_str_attr(
                TagId::Cv,
                cv_node_id,
                cv_list_node_id,
                ACC_ATTR_CV_FULL_NAME,
                name,
            );
        }
        if let Some(ver) = cv.version.as_deref().filter(|s| !s.is_empty()) {
            sink.push_str_attr(
                TagId::Cv,
                cv_node_id,
                cv_list_node_id,
                ACC_ATTR_CV_VERSION,
                ver,
            );
        }
        if let Some(uri) = cv.uri.as_deref().filter(|s| !s.is_empty()) {
            sink.push_str_attr(TagId::Cv, cv_node_id, cv_list_node_id, ACC_ATTR_CV_URI, uri);
        }

        commit_global_meta_item(items, cvs, tags, owners, parents);
    }
}

fn collect_spectrum_meta(
    sink: &mut MetadataCollector<'_>,
    spectrum: &Spectrum,
    ref_group_lookup: &HashMap<&str, &ReferenceableParamGroup>,
    spectrum_node_id: u32,
    node_ids: &mut NodeIdAllocator,
    x_array_accession: u32,
    y_array_accession: u32,
    force_f32: bool,
) {
    sink.push_ref_group_params(
        spectrum_node_id,
        0,
        &spectrum.referenceable_param_group_refs,
        ref_group_lookup,
    );

    if let Some(desc) = &spectrum.spectrum_description {
        let desc_id = node_ids.next();
        sink.touch(TagId::SpectrumDescription, desc_id, spectrum_node_id);
        sink.push_ref_group_params(
            desc_id,
            spectrum_node_id,
            &desc.referenceable_param_group_refs,
            ref_group_lookup,
        );
        sink.push_cv_and_user_params(
            desc_id,
            spectrum_node_id,
            &desc.cv_params,
            &desc.user_params,
        );

        if let Some(scan_list) = &desc.scan_list {
            let sl_id = node_ids.next();
            sink.push_optional_u32_attr(
                TagId::ScanList,
                sl_id,
                desc_id,
                ACC_ATTR_COUNT,
                Some(scan_list.scans.len() as u32),
            );
            sink.push_cv_and_user_params(
                sl_id,
                desc_id,
                &scan_list.cv_params,
                &scan_list.user_params,
            );
            collect_scan_list(sink, scan_list, sl_id, node_ids);
        }
        if let Some(pl) = &desc.precursor_list {
            let pl_id = node_ids.next();
            sink.push_optional_u32_attr(
                TagId::PrecursorList,
                pl_id,
                desc_id,
                ACC_ATTR_COUNT,
                Some(pl.precursors.len() as u32),
            );
            for p in &pl.precursors {
                collect_precursor(sink, p, pl_id, node_ids);
            }
        }
        if let Some(prod_list) = &desc.product_list {
            let prod_list_id = node_ids.next();
            sink.push_optional_u32_attr(
                TagId::ProductList,
                prod_list_id,
                desc_id,
                ACC_ATTR_COUNT,
                Some(prod_list.products.len() as u32),
            );
            for p in &prod_list.products {
                collect_product(sink, p, prod_list_id, node_ids);
            }
        }
    }

    if let Some(scan_list) = &spectrum.scan_list {
        let sl_id = node_ids.next();
        sink.push_optional_u32_attr(
            TagId::ScanList,
            sl_id,
            spectrum_node_id,
            ACC_ATTR_COUNT,
            Some(scan_list.scans.len() as u32),
        );
        sink.push_cv_and_user_params(
            sl_id,
            spectrum_node_id,
            &scan_list.cv_params,
            &scan_list.user_params,
        );
        collect_scan_list(sink, scan_list, sl_id, node_ids);
    }
    if let Some(pl) = &spectrum.precursor_list {
        let pl_id = node_ids.next();
        sink.push_optional_u32_attr(
            TagId::PrecursorList,
            pl_id,
            spectrum_node_id,
            ACC_ATTR_COUNT,
            Some(pl.precursors.len() as u32),
        );
        for p in &pl.precursors {
            collect_precursor(sink, p, pl_id, node_ids);
        }
    }
    if let Some(prod_list) = &spectrum.product_list {
        let prod_list_id = node_ids.next();
        sink.push_optional_u32_attr(
            TagId::ProductList,
            prod_list_id,
            spectrum_node_id,
            ACC_ATTR_COUNT,
            Some(prod_list.products.len() as u32),
        );
        for p in &prod_list.products {
            collect_product(sink, p, prod_list_id, node_ids);
        }
    }

    collect_binary_data_array_list(
        sink,
        spectrum.binary_data_array_list.as_ref(),
        spectrum_node_id,
        node_ids,
        x_array_accession,
        y_array_accession,
        force_f32,
    );
}

fn collect_chromatogram_meta(
    sink: &mut MetadataCollector<'_>,
    chrom: &Chromatogram,
    chrom_node_id: u32,
    node_ids: &mut NodeIdAllocator,
    x_array_accession: u32,
    y_array_accession: u32,
    force_f32: bool,
) {
    if let Some(p) = &chrom.precursor {
        collect_precursor(sink, p, chrom_node_id, node_ids);
    }
    if let Some(p) = &chrom.product {
        collect_product(sink, p, chrom_node_id, node_ids);
    }
    collect_binary_data_array_list(
        sink,
        chrom.binary_data_array_list.as_ref(),
        chrom_node_id,
        node_ids,
        x_array_accession,
        y_array_accession,
        force_f32,
    );
}

fn collect_binary_data_array_list(
    sink: &mut MetadataCollector<'_>,
    bda_list: Option<&crate::mzml::structs::BinaryDataArrayList>,
    parent_node_id: u32,
    node_ids: &mut NodeIdAllocator,
    x_array_accession: u32,
    y_array_accession: u32,
    force_f32: bool,
) {
    let Some(bda_list) = bda_list else { return };

    let bda_list_node_id = node_ids.next();
    sink.push_optional_u32_attr(
        TagId::BinaryDataArrayList,
        bda_list_node_id,
        parent_node_id,
        ACC_ATTR_COUNT,
        Some(bda_list.binary_data_arrays.len() as u32),
    );

    for bda in &bda_list.binary_data_arrays {
        let bda_node_id = node_ids.next();
        sink.touch(TagId::BinaryDataArray, bda_node_id, bda_list_node_id);
        sink.push_schema_attrs(TagId::BinaryDataArray, bda_node_id, bda_list_node_id, bda);
        emit_bda_cvparams_with_float_precision_override(
            sink,
            bda_node_id,
            bda_list_node_id,
            bda,
            x_array_accession,
            y_array_accession,
            force_f32,
        );
    }
}

fn collect_precursor(
    sink: &mut MetadataCollector<'_>,
    precursor: &Precursor,
    parent_node_id: u32,
    node_ids: &mut NodeIdAllocator,
) {
    let precursor_id = node_ids.next();
    sink.touch(TagId::Precursor, precursor_id, parent_node_id);
    sink.push_schema_attrs(TagId::Precursor, precursor_id, parent_node_id, precursor);

    if let Some(iw) = &precursor.isolation_window {
        let iw_id = node_ids.next();
        sink.touch(TagId::IsolationWindow, iw_id, precursor_id);
        sink.push_cv_and_user_params(iw_id, precursor_id, &iw.cv_params, &iw.user_params);
    }

    if let Some(sil) = &precursor.selected_ion_list {
        let sil_id = node_ids.next();
        sink.push_optional_u32_attr(
            TagId::SelectedIonList,
            sil_id,
            precursor_id,
            ACC_ATTR_COUNT,
            Some(sil.selected_ions.len() as u32),
        );
        for si in &sil.selected_ions {
            let si_id = node_ids.next();
            sink.touch(TagId::SelectedIon, si_id, sil_id);
            sink.push_cv_and_user_params(si_id, sil_id, &si.cv_params, &si.user_params);
        }
    }

    if let Some(act) = &precursor.activation {
        let act_id = node_ids.next();
        sink.touch(TagId::Activation, act_id, precursor_id);
        sink.push_cv_and_user_params(act_id, precursor_id, &act.cv_params, &act.user_params);
    }
}

fn collect_product(
    sink: &mut MetadataCollector<'_>,
    product: &Product,
    parent_node_id: u32,
    node_ids: &mut NodeIdAllocator,
) {
    let product_id = node_ids.next();
    sink.touch(TagId::Product, product_id, parent_node_id);
    sink.push_schema_attrs(TagId::Product, product_id, parent_node_id, product);
    sink.push_cv_and_user_params(
        product_id,
        parent_node_id,
        &product.cv_params,
        &product.user_params,
    );

    if let Some(iw) = &product.isolation_window {
        let iw_id = node_ids.next();
        sink.touch(TagId::IsolationWindow, iw_id, product_id);
        sink.push_cv_and_user_params(iw_id, product_id, &iw.cv_params, &iw.user_params);
    }
}

fn collect_scan_list(
    sink: &mut MetadataCollector<'_>,
    scan_list: &ScanList,
    scan_list_node_id: u32,
    node_ids: &mut NodeIdAllocator,
) {
    for scan in &scan_list.scans {
        let scan_id = node_ids.next();
        sink.touch(TagId::Scan, scan_id, scan_list_node_id);
        sink.push_schema_attrs(TagId::Scan, scan_id, scan_list_node_id, scan);
        sink.push_cv_and_user_params(
            scan_id,
            scan_list_node_id,
            &scan.cv_params,
            &scan.user_params,
        );

        if let Some(swl) = &scan.scan_window_list {
            let swl_id = node_ids.next();
            sink.push_optional_u32_attr(
                TagId::ScanWindowList,
                swl_id,
                scan_id,
                ACC_ATTR_COUNT,
                Some(swl.scan_windows.len() as u32),
            );
            for sw in &swl.scan_windows {
                let sw_id = node_ids.next();
                sink.touch(TagId::ScanWindow, sw_id, swl_id);
                sink.push_cv_and_user_params(sw_id, swl_id, &sw.cv_params, &sw.user_params);
            }
        }
    }
}

fn make_compression_mode(compression_level: u8) -> CompressionMode<DefaultCompressor> {
    if compression_level > 0 {
        CompressionMode::Compressed(DefaultCompressor::new(compression_level as i32).unwrap())
    } else {
        CompressionMode::Raw
    }
}

fn make_filter_type(byte_shuffle_enabled: bool) -> FilterType {
    if byte_shuffle_enabled {
        FilterType::Shuffle
    } else {
        FilterType::None
    }
}

pub fn encode(mzml: &MzML, compression_level: u8, force_f32: bool) -> Vec<u8> {
    assert!(compression_level <= 22);

    let do_compress = compression_level != 0;
    let do_byte_shuffle = do_compress;
    let array_filter_id = if do_byte_shuffle {
        ARRAY_FILTER_BYTE_SHUFFLE
    } else {
        ARRAY_FILTER_NONE
    };
    let codec_id: u8 = if do_compress { 1 } else { 0 };

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

    let ref_group_lookup = build_ref_group_lookup(mzml);
    let mut node_ids = NodeIdAllocator::new();
    let mut row_ids = RowIdAllocator::new();

    let spectrum_list_node_id: u32 = if run.spectrum_list.is_some() {
        node_ids.next()
    } else {
        0
    };
    let chrom_list_node_id: u32 = if run.chromatogram_list.is_some() {
        node_ids.next()
    } else {
        0
    };

    let (mut global_items, global_counts) =
        build_global_meta_items(mzml, &ref_group_lookup, &mut node_ids);
    for item in &mut global_items {
        normalize_attr_cv_values(&mut item.cv_params);
    }

    let mut spectrum_index: usize = 0;
    let spectrum_meta = build_metadata_block(&mut row_ids, spectra, |sink, spectrum| {
        let current_index = spectrum_index;
        spectrum_index += 1;

        if current_index == 0 && spectrum_list_node_id != 0 {
            let sl = run.spectrum_list.as_ref().unwrap();
            sink.touch(TagId::SpectrumList, spectrum_list_node_id, 0);
            sink.push_schema_attrs(TagId::SpectrumList, spectrum_list_node_id, 0, sl);
        }

        let spectrum_node_id = node_ids.next();
        sink.push_schema_attrs(
            TagId::Spectrum,
            spectrum_node_id,
            spectrum_list_node_id,
            spectrum,
        );
        if spectrum.index.is_none() {
            sink.push_optional_u32_attr(
                TagId::Spectrum,
                spectrum_node_id,
                spectrum_list_node_id,
                ACC_ATTR_INDEX,
                Some(current_index as u32),
            );
        }
        sink.push_ref_group_params(
            spectrum_node_id,
            spectrum_list_node_id,
            &spectrum.referenceable_param_group_refs,
            &ref_group_lookup,
        );
        sink.push_cv_and_user_params(
            spectrum_node_id,
            spectrum_list_node_id,
            &spectrum.cv_params,
            &spectrum.user_params,
        );

        collect_spectrum_meta(
            sink,
            spectrum,
            &ref_group_lookup,
            spectrum_node_id,
            &mut node_ids,
            ACCESSION_MZ_ARRAY,
            ACCESSION_INTENSITY_ARRAY,
            force_f32,
        );
        normalize_attr_cv_values(sink.cv_params);
    });

    let mut chrom_index: usize = 0;
    let chrom_meta = build_metadata_block(&mut row_ids, chromatograms, |sink, chrom| {
        let current_index = chrom_index;
        chrom_index += 1;

        if current_index == 0 && chrom_list_node_id != 0 {
            let cl = run.chromatogram_list.as_ref().unwrap();
            sink.touch(TagId::ChromatogramList, chrom_list_node_id, 0);
            sink.push_schema_attrs(TagId::ChromatogramList, chrom_list_node_id, 0, cl);
        }

        let chrom_node_id = node_ids.next();
        sink.push_schema_attrs(
            TagId::Chromatogram,
            chrom_node_id,
            chrom_list_node_id,
            chrom,
        );
        sink.push_ref_group_params(
            chrom_node_id,
            chrom_list_node_id,
            &chrom.referenceable_param_group_refs,
            &ref_group_lookup,
        );
        sink.push_cv_and_user_params(
            chrom_node_id,
            chrom_list_node_id,
            &chrom.cv_params,
            &chrom.user_params,
        );

        collect_chromatogram_meta(
            sink,
            chrom,
            chrom_node_id,
            &mut node_ids,
            ACCESSION_TIME_ARRAY,
            ACCESSION_INTENSITY_ARRAY,
            force_f32,
        );
        normalize_attr_cv_values(sink.cv_params);
    });

    let global_meta = pack_meta_from_slices(&mut row_ids, &global_items, |item| {
        (
            item.cv_params.as_slice(),
            item.tag_ids.as_slice(),
            item.owner_ids.as_slice(),
            item.parent_ids.as_slice(),
        )
    });

    let mut spectrum_meta_bytes = serialize_packed_meta(&spectrum_meta);
    let mut chrom_meta_bytes = serialize_packed_meta(&chrom_meta);
    let mut global_meta_bytes = serialize_global_meta_with_counts(&global_counts, &global_meta);

    let spectrum_meta_uncompressed_size = spectrum_meta_bytes.len() as u64;
    let chrom_meta_uncompressed_size = chrom_meta_bytes.len() as u64;
    let global_meta_uncompressed_size = global_meta_bytes.len() as u64;

    if do_compress {
        spectrum_meta_bytes = compress_if_needed(&spectrum_meta_bytes, compression_level);
        chrom_meta_bytes = compress_if_needed(&chrom_meta_bytes, compression_level);
        global_meta_bytes = compress_if_needed(&global_meta_bytes, compression_level);
    }

    let (packed_spectra, spectrum_block_count, spec_entries, spec_arrayrefs, spec_array_types) =
        pack_arrays(
            spectra,
            &mut node_ids,
            compression_level,
            do_byte_shuffle,
            force_f32,
            ACCESSION_MZ_ARRAY,
            ACCESSION_INTENSITY_ARRAY,
        );

    let (packed_chroms, chrom_block_count, chrom_entries, chrom_arrayrefs, chrom_array_types) =
        pack_arrays(
            chromatograms,
            &mut node_ids,
            compression_level,
            do_byte_shuffle,
            force_f32,
            ACCESSION_TIME_ARRAY,
            ACCESSION_INTENSITY_ARRAY,
        );

    let mut output =
        Vec::with_capacity(HEADER_SIZE + packed_spectra.len() + packed_chroms.len() + 1024);
    output.resize(HEADER_SIZE, 0);

    let offset_spec_entries = append_8byte_aligned(&mut output, &spec_entries);
    let offset_spec_arrayrefs = append_8byte_aligned(&mut output, &spec_arrayrefs);
    let offset_chrom_entries = append_8byte_aligned(&mut output, &chrom_entries);
    let offset_chrom_arrayrefs = append_8byte_aligned(&mut output, &chrom_arrayrefs);
    let offset_spec_meta = append_8byte_aligned(&mut output, &spectrum_meta_bytes);
    let offset_chrom_meta = append_8byte_aligned(&mut output, &chrom_meta_bytes);
    let offset_global_meta = append_8byte_aligned(&mut output, &global_meta_bytes);
    let offset_packed_spectra = append_8byte_aligned(&mut output, &packed_spectra);
    let offset_packed_chroms = append_8byte_aligned(&mut output, &packed_chroms);

    write_file_header(
        &mut output,
        offset_spec_entries,
        &spec_entries,
        offset_spec_arrayrefs,
        &spec_arrayrefs,
        offset_chrom_entries,
        &chrom_entries,
        offset_chrom_arrayrefs,
        &chrom_arrayrefs,
        offset_spec_meta,
        &spectrum_meta_bytes,
        offset_chrom_meta,
        &chrom_meta_bytes,
        offset_global_meta,
        &global_meta_bytes,
        offset_packed_spectra,
        &packed_spectra,
        offset_packed_chroms,
        &packed_chroms,
        spectrum_block_count,
        chrom_block_count,
        spectrum_count,
        chrom_count,
        &spectrum_meta,
        &chrom_meta,
        &global_meta,
        spec_array_types.len() as u32,
        chrom_array_types.len() as u32,
        codec_id,
        compression_level,
        array_filter_id,
        spectrum_meta_uncompressed_size,
        chrom_meta_uncompressed_size,
        global_meta_uncompressed_size,
    );

    output.extend_from_slice(&FILE_TRAILER);
    output
}

trait HasBinaryDataArrayList {
    fn binary_data_array_list(&self) -> Option<&crate::mzml::structs::BinaryDataArrayList>;
}

impl HasBinaryDataArrayList for Spectrum {
    fn binary_data_array_list(&self) -> Option<&crate::mzml::structs::BinaryDataArrayList> {
        self.binary_data_array_list.as_ref()
    }
}

impl HasBinaryDataArrayList for Chromatogram {
    fn binary_data_array_list(&self) -> Option<&crate::mzml::structs::BinaryDataArrayList> {
        self.binary_data_array_list.as_ref()
    }
}

fn pack_arrays<T: HasBinaryDataArrayList>(
    items: &[T],
    _node_ids: &mut NodeIdAllocator,
    compression_level: u8,
    byte_shuffle_enabled: bool,
    force_f32: bool,
    x_array_accession: u32,
    y_array_accession: u32,
) -> (Vec<u8>, u32, Vec<u8>, Vec<u8>, HashSet<u32>) {
    let mut entries_buf = Vec::new();
    let mut arrayrefs_buf = Vec::new();
    let mut seen_array_type_accessions: HashSet<u32> = HashSet::new();
    let mut arrayref_cursor: u64 = 0;

    let mut builder = ContainerBuilder::new(
        TARGET_BLOCK_UNCOMPRESSED_BYTES,
        make_compression_mode(compression_level),
        make_filter_type(byte_shuffle_enabled),
    );

    for item in items {
        let arrayref_start = arrayref_cursor;
        let mut arrayref_count: u64 = 0;

        if let Some(bda_list) = item.binary_data_array_list() {
            for bda in &bda_list.binary_data_arrays {
                let Some(data) = array_data_from_binary_data_array(bda) else {
                    continue;
                };
                if data.is_empty() {
                    continue;
                }

                let array_type_accession = array_type_accession_from_bda(bda);
                if array_type_accession != 0 {
                    seen_array_type_accessions.insert(array_type_accession);
                }

                let is_x_or_y_array = array_type_accession == x_array_accession
                    || array_type_accession == y_array_accession;
                let effective_force_f32 = force_f32 && is_x_or_y_array;

                let writer_dtype = resolve_write_dtype(bda, data, effective_force_f32);
                let entry_dtype = writer_dtype_to_entry_dtype(writer_dtype);
                let element_size = element_size_for_dtype(writer_dtype);

                let (block_id, element_offset) = builder
                    .add_item_to_box(data.len() * element_size, element_size, |buf| {
                        write_array_data(buf, data, writer_dtype);
                    })
                    .unwrap();

                write_arrayref_entry(
                    &mut arrayrefs_buf,
                    element_offset,
                    data.len() as u64,
                    block_id,
                    array_type_accession,
                    entry_dtype,
                );
                arrayref_cursor += 1;
                arrayref_count += 1;
            }
        }

        write_u64_le(&mut entries_buf, arrayref_start);
        write_u64_le(&mut entries_buf, arrayref_count);
    }

    let (container, block_count) = builder.pack().unwrap();
    (
        container,
        block_count,
        entries_buf,
        arrayrefs_buf,
        seen_array_type_accessions,
    )
}

fn write_arrayref_entry(
    buf: &mut Vec<u8>,
    element_offset: u64,
    element_count: u64,
    block_id: u32,
    array_type_accession: u32,
    dtype: u8,
) {
    write_u64_le(buf, element_offset);
    write_u64_le(buf, element_count);
    write_u32_le(buf, block_id);
    write_u32_le(buf, array_type_accession);
    buf.push(dtype);
    buf.extend_from_slice(&[0u8; 7]);
}

#[allow(clippy::too_many_arguments)]
fn write_file_header(
    output: &mut Vec<u8>,
    offset_spec_entries: u64,
    spec_entries: &[u8],
    offset_spec_arrayrefs: u64,
    spec_arrayrefs: &[u8],
    offset_chrom_entries: u64,
    chrom_entries: &[u8],
    offset_chrom_arrayrefs: u64,
    chrom_arrayrefs: &[u8],
    offset_spec_meta: u64,
    spec_meta_bytes: &[u8],
    offset_chrom_meta: u64,
    chrom_meta_bytes: &[u8],
    offset_global_meta: u64,
    global_meta_bytes: &[u8],
    offset_packed_spectra: u64,
    packed_spectra: &[u8],
    offset_packed_chroms: u64,
    packed_chroms: &[u8],
    spectrum_block_count: u32,
    chrom_block_count: u32,
    spectrum_count: u32,
    chrom_count: u32,
    spectrum_meta: &MetadataBlock,
    chrom_meta: &MetadataBlock,
    global_meta: &MetadataBlock,
    spec_array_type_count: u32,
    chrom_array_type_count: u32,
    codec_id: u8,
    compression_level: u8,
    array_filter_id: u8,
    spec_meta_uncompressed_size: u64,
    chrom_meta_uncompressed_size: u64,
    global_meta_uncompressed_size: u64,
) {
    let header = &mut output[0..HEADER_SIZE];
    header[0..4].copy_from_slice(b"B000");
    patch_u64_at(header, 8, offset_spec_entries);
    patch_u64_at(header, 16, spec_entries.len() as u64);
    patch_u64_at(header, 24, offset_spec_arrayrefs);
    patch_u64_at(header, 32, spec_arrayrefs.len() as u64);
    patch_u64_at(header, 40, offset_chrom_entries);
    patch_u64_at(header, 48, chrom_entries.len() as u64);
    patch_u64_at(header, 56, offset_chrom_arrayrefs);
    patch_u64_at(header, 64, chrom_arrayrefs.len() as u64);
    patch_u64_at(header, 72, offset_spec_meta);
    patch_u64_at(header, 80, spec_meta_bytes.len() as u64);
    patch_u64_at(header, 88, offset_chrom_meta);
    patch_u64_at(header, 96, chrom_meta_bytes.len() as u64);
    patch_u64_at(header, 104, offset_global_meta);
    patch_u64_at(header, 112, global_meta_bytes.len() as u64);
    patch_u64_at(header, 120, offset_packed_spectra);
    patch_u64_at(header, 128, packed_spectra.len() as u64);
    patch_u64_at(header, 136, offset_packed_chroms);
    patch_u64_at(header, 144, packed_chroms.len() as u64);
    patch_u32_at(header, 152, spectrum_block_count);
    patch_u32_at(header, 156, chrom_block_count);
    patch_u32_at(header, 160, spectrum_count);
    patch_u32_at(header, 164, chrom_count);
    patch_u32_at(header, 168, spectrum_meta.ref_codes.len() as u32);
    patch_u32_at(header, 172, spectrum_meta.numeric_values.len() as u32);
    patch_u32_at(header, 176, spectrum_meta.string_offsets.len() as u32);
    patch_u32_at(header, 180, chrom_meta.ref_codes.len() as u32);
    patch_u32_at(header, 184, chrom_meta.numeric_values.len() as u32);
    patch_u32_at(header, 188, chrom_meta.string_offsets.len() as u32);
    patch_u32_at(header, 192, global_meta.ref_codes.len() as u32);
    patch_u32_at(header, 196, global_meta.numeric_values.len() as u32);
    patch_u32_at(header, 200, global_meta.string_offsets.len() as u32);
    patch_u32_at(header, 204, spec_array_type_count);
    patch_u32_at(header, 208, chrom_array_type_count);
    patch_u64_at(header, 216, TARGET_BLOCK_UNCOMPRESSED_BYTES as u64);
    patch_u8_at(header, 224, codec_id);
    patch_u8_at(header, 225, compression_level);
    patch_u8_at(header, 226, array_filter_id);
    patch_u64_at(header, 232, spec_meta_uncompressed_size);
    patch_u64_at(header, 240, chrom_meta_uncompressed_size);
    patch_u64_at(header, 248, global_meta_uncompressed_size);
}
