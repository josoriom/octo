use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    slice,
};
use zstd::bulk::compress as zstd_compress;

use crate::{
    BinaryData, NumericType,
    b64::utilities::{assign_attributes, container_builder::ContainerBuilder},
    decode::MetadatumValue,
    mzml::{
        attr_meta::*,
        schema::TagId,
        structs::{
            BinaryDataArray, Chromatogram, CvParam, MzML, Precursor, Product,
            ReferenceableParamGroup, ReferenceableParamGroupRef, ScanList, Spectrum,
        },
    },
};

#[derive(Debug)]
pub struct PackedMeta {
    pub index_offsets: Vec<u32>,
    pub ids: Vec<u32>,
    pub parent_indices: Vec<u32>,
    pub tag_ids: Vec<u8>,
    pub ref_codes: Vec<u8>,
    pub accession_numbers: Vec<u32>,
    pub unit_ref_codes: Vec<u8>,
    pub unit_accession_numbers: Vec<u32>,
    pub value_kinds: Vec<u8>,
    pub value_indices: Vec<u32>,
    pub numeric_values: Vec<f64>,
    pub string_offsets: Vec<u32>,
    pub string_lengths: Vec<u32>,
    pub string_bytes: Vec<u8>,
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

#[derive(Debug, Default)]
struct RowIdGen {
    next: u32,
}

impl RowIdGen {
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
    fn push_tagged_raw(&mut self, tag_id: u8, id: u32, parent_id: u32, cv: CvParam) {
        self.out.push(cv);
        self.tags.push(tag_id);
        self.owners.push(id);
        self.parents.push(parent_id);
    }

    #[inline]
    fn extend_tagged_raw(&mut self, tag_id: u8, id: u32, parent_id: u32, cvs: &[CvParam]) {
        self.out.extend_from_slice(cvs);
        self.tags.resize(self.tags.len() + cvs.len(), tag_id);
        self.owners.resize(self.owners.len() + cvs.len(), id);
        self.parents
            .resize(self.parents.len() + cvs.len(), parent_id);
    }

    #[inline]
    fn push_tagged_ids(&mut self, tag: TagId, id: u32, parent_id: u32, cv: CvParam) {
        self.push_tagged_raw(tag as u8, id, parent_id, cv);
    }

    #[inline]
    fn extend_tagged_ids(&mut self, tag: TagId, id: u32, parent_id: u32, cvs: &[CvParam]) {
        self.extend_tagged_raw(tag as u8, id, parent_id, cvs);
    }

    #[inline]
    fn touch_tagged_ids(&mut self, tag: TagId, id: u32, parent_id: u32) {
        self.push_tagged_ids(
            tag,
            id,
            parent_id,
            CvParam {
                cv_ref: None,
                accession: None,
                name: String::new(),
                value: None,
                unit_cv_ref: None,
                unit_name: None,
                unit_accession: None,
            },
        );
    }

    #[inline]
    fn push_attr_string_tagged_raw(
        &mut self,
        tag_id: u8,
        id: u32,
        parent_id: u32,
        accession_tail: u32,
        value: &str,
    ) {
        if !value.is_empty() {
            self.push_tagged_raw(tag_id, id, parent_id, attr_cv_param(accession_tail, value));
        }
    }

    #[inline]
    fn push_attr_u32_tagged_raw(
        &mut self,
        tag_id: u8,
        id: u32,
        parent_id: u32,
        accession_tail: u32,
        value: Option<u32>,
    ) {
        if let Some(v) = value {
            self.push_tagged_raw(
                tag_id,
                id,
                parent_id,
                attr_cv_param(accession_tail, &v.to_string()),
            );
        }
    }

    #[inline]
    fn push_attr_string_tagged_ids(
        &mut self,
        tag: TagId,
        id: u32,
        parent_id: u32,
        accession_tail: u32,
        value: &str,
    ) {
        self.push_attr_string_tagged_raw(tag as u8, id, parent_id, accession_tail, value);
    }

    #[inline]
    fn push_attr_usize_tagged_ids(
        &mut self,
        tag: TagId,
        id: u32,
        parent_id: u32,
        accession_tail: u32,
        value: Option<u32>,
    ) {
        self.push_attr_u32_tagged_raw(tag as u8, id, parent_id, accession_tail, value);
    }

    #[inline]
    fn extend_ref_group_cv_params_ids(
        &mut self,
        _owner_tag: TagId,
        owner_element_id: u32,
        owner_parent_element_id: u32,
        refs: &[ReferenceableParamGroupRef],
        ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    ) {
        for r in refs {
            if let Some(g) = ref_groups.get(r.r#ref.as_str()) {
                self.extend_tagged_ids(
                    TagId::CvParam,
                    owner_element_id,
                    owner_parent_element_id,
                    &g.cv_params,
                );
            }
        }
    }

    #[inline]
    fn push_schema_attributes<T: Serialize>(
        &mut self,
        tag: TagId,
        id: u32,
        parent_id: u32,
        expected: &T,
    ) {
        let attrs = assign_attributes(expected, tag, id, parent_id);
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

            self.push_tagged_ids(tag, id, parent_id, attr_cv_param(tail, &s));
        }
    }
}

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

fn extend_binary_data_array_cv_params_ids(
    meta: &mut MetaAcc<'_>,
    binary_data_array_id: u32,
    binary_data_array_list_id: u32,
    binary_data_array: &BinaryDataArray,
    x_accession_tail: u32,
    y_accession_tail: u32,
    f32_compress: bool,
) {
    let mut is_x_array = false;
    let mut is_y_array = false;
    for cv in &binary_data_array.cv_params {
        let tail = parse_accession_tail(cv.accession.as_deref());
        if tail == x_accession_tail {
            is_x_array = true;
        } else if tail == y_accession_tail {
            is_y_array = true;
        }
    }

    let desired_float_tail = if f32_compress && (is_x_array || is_y_array) {
        Some(ACC_32BIT_FLOAT)
    } else {
        None
    };

    if let Some(desired) = desired_float_tail {
        let mut wrote_float = false;
        for cv in &binary_data_array.cv_params {
            let tail = parse_accession_tail(cv.accession.as_deref());
            if tail == ACC_32BIT_FLOAT || tail == ACC_64BIT_FLOAT {
                if !wrote_float {
                    meta.push_tagged_ids(
                        TagId::CvParam,
                        binary_data_array_id,
                        binary_data_array_list_id,
                        ms_float_param(desired),
                    );
                    wrote_float = true;
                }
                continue;
            }
            meta.push_tagged_ids(
                TagId::CvParam,
                binary_data_array_id,
                binary_data_array_list_id,
                cv.clone(),
            );
        }
        if !wrote_float {
            meta.push_tagged_ids(
                TagId::CvParam,
                binary_data_array_id,
                binary_data_array_list_id,
                ms_float_param(desired),
            );
        }
    } else {
        meta.extend_tagged_ids(
            TagId::CvParam,
            binary_data_array_id,
            binary_data_array_list_id,
            &binary_data_array.cv_params,
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

fn build_global_meta_items(
    mzml: &MzML,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    id_gen: &mut NodeIdGen,
) -> (Vec<GlobalMetaItem>, GlobalCounts) {
    let mut items: Vec<GlobalMetaItem> = Vec::new();

    if let Some(file_description) = &mzml.file_description {
        let mut out = Vec::new();
        let mut tags = Vec::new();
        let mut owners = Vec::new();
        let mut parents = Vec::new();
        let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

        let file_description_id = id_gen.alloc();
        meta.touch_tagged_ids(TagId::FileDescription, file_description_id, 0);

        let file_content_id = id_gen.alloc();
        meta.touch_tagged_ids(TagId::FileContent, file_content_id, file_description_id);

        meta.extend_ref_group_cv_params_ids(
            TagId::FileContent,
            file_content_id,
            file_description_id,
            &file_description.file_content.referenceable_param_group_refs,
            ref_groups,
        );

        meta.extend_tagged_ids(
            TagId::CvParam,
            file_content_id,
            file_description_id,
            &file_description.file_content.cv_params,
        );

        let source_file_list_id = id_gen.alloc();
        meta.touch_tagged_ids(
            TagId::SourceFileList,
            source_file_list_id,
            file_description_id,
        );

        meta.push_attr_usize_tagged_ids(
            TagId::SourceFileList,
            source_file_list_id,
            file_description_id,
            ACC_ATTR_COUNT,
            Some(file_description.source_file_list.source_file.len() as u32),
        );

        for source_file in &file_description.source_file_list.source_file {
            let source_file_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::SourceFile, source_file_id, source_file_list_id);

            meta.push_attr_string_tagged_ids(
                TagId::SourceFile,
                source_file_id,
                source_file_list_id,
                ACC_ATTR_ID,
                source_file.id.as_str(),
            );

            meta.push_attr_string_tagged_ids(
                TagId::SourceFile,
                source_file_id,
                source_file_list_id,
                ACC_ATTR_NAME,
                source_file.name.as_str(),
            );

            meta.push_attr_string_tagged_ids(
                TagId::SourceFile,
                source_file_id,
                source_file_list_id,
                ACC_ATTR_LOCATION,
                source_file.location.as_str(),
            );

            meta.extend_ref_group_cv_params_ids(
                TagId::SourceFile,
                source_file_id,
                source_file_list_id,
                &source_file.referenceable_param_group_ref,
                ref_groups,
            );

            meta.extend_tagged_ids(
                TagId::CvParam,
                source_file_id,
                source_file_list_id,
                &source_file.cv_param,
            );
        }

        for contact in &file_description.contacts {
            let contact_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::Contact, contact_id, file_description_id);

            meta.extend_ref_group_cv_params_ids(
                TagId::Contact,
                contact_id,
                file_description_id,
                &contact.referenceable_param_group_refs,
                ref_groups,
            );

            meta.extend_tagged_ids(
                TagId::CvParam,
                contact_id,
                file_description_id,
                &contact.cv_params,
            );
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
        meta.touch_tagged_ids(TagId::Run, run_id, 0);

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

        if let Some(source_file_ref_list) = &run.source_file_ref_list {
            let source_file_ref_list_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::SourceFileRefList, source_file_ref_list_id, run_id);

            meta.push_attr_usize_tagged_ids(
                TagId::SourceFileRefList,
                source_file_ref_list_id,
                run_id,
                ACC_ATTR_COUNT,
                Some(source_file_ref_list.source_file_refs.len() as u32),
            );

            for source_file_ref in &source_file_ref_list.source_file_refs {
                let source_file_ref_id = id_gen.alloc();
                meta.touch_tagged_ids(
                    TagId::SourceFileRef,
                    source_file_ref_id,
                    source_file_ref_list_id,
                );

                meta.push_attr_string_tagged_ids(
                    TagId::SourceFileRef,
                    source_file_ref_id,
                    source_file_ref_list_id,
                    ACC_ATTR_REF,
                    source_file_ref.r#ref.as_str(),
                );
            }
        }

        for referenceable_group_ref in &run.referenceable_param_group_refs {
            let referenceable_group_ref_id = id_gen.alloc();
            meta.touch_tagged_ids(
                TagId::ReferenceableParamGroupRef,
                referenceable_group_ref_id,
                run_id,
            );

            meta.push_attr_string_tagged_ids(
                TagId::ReferenceableParamGroupRef,
                referenceable_group_ref_id,
                run_id,
                ACC_ATTR_REF,
                referenceable_group_ref.r#ref.as_str(),
            );
        }

        if !run.cv_params.is_empty() {
            meta.extend_tagged_ids(TagId::CvParam, run_id, 0, &run.cv_params);
        }

        items.push(GlobalMetaItem {
            cvs: out,
            tags,
            owners,
            parents,
        });
    }

    let ref_start = items.len();
    if let Some(referenceable_param_group_list) = &mzml.referenceable_param_group_list {
        for group in &referenceable_param_group_list.referenceable_param_groups {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let group_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::ReferenceableParamGroup, group_id, 0);

            meta.push_attr_string_tagged_ids(
                TagId::ReferenceableParamGroup,
                group_id,
                0,
                ACC_ATTR_ID,
                group.id.as_str(),
            );

            meta.extend_tagged_ids(TagId::CvParam, group_id, 0, &group.cv_params);

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
    if let Some(sample_list) = &mzml.sample_list {
        for sample in &sample_list.samples {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let sample_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::Sample, sample_id, 0);

            meta.push_attr_string_tagged_ids(
                TagId::Sample,
                sample_id,
                0,
                ACC_ATTR_ID,
                sample.id.as_str(),
            );

            meta.push_attr_string_tagged_ids(
                TagId::Sample,
                sample_id,
                0,
                ACC_ATTR_NAME,
                sample.name.as_str(),
            );

            if let Some(r) = &sample.referenceable_param_group_ref {
                meta.extend_ref_group_cv_params_ids(
                    TagId::Sample,
                    sample_id,
                    0,
                    slice::from_ref(r),
                    ref_groups,
                );
            }

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
    if let Some(instrument_list) = &mzml.instrument_list {
        for instrument_config in &instrument_list.instrument {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let instrument_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::Instrument, instrument_id, 0);

            meta.push_attr_string_tagged_ids(
                TagId::Instrument,
                instrument_id,
                0,
                ACC_ATTR_ID,
                instrument_config.id.as_str(),
            );

            meta.extend_ref_group_cv_params_ids(
                TagId::Instrument,
                instrument_id,
                0,
                &instrument_config.referenceable_param_group_ref,
                ref_groups,
            );

            meta.extend_tagged_ids(
                TagId::CvParam,
                instrument_id,
                0,
                &instrument_config.cv_param,
            );

            if let Some(component_list) = &instrument_config.component_list {
                for src in &component_list.source {
                    let component_id = id_gen.alloc();
                    meta.touch_tagged_ids(TagId::ComponentSource, component_id, instrument_id);

                    meta.push_attr_usize_tagged_ids(
                        TagId::ComponentSource,
                        component_id,
                        instrument_id,
                        ACC_ATTR_ORDER,
                        src.order,
                    );

                    meta.extend_ref_group_cv_params_ids(
                        TagId::ComponentSource,
                        component_id,
                        instrument_id,
                        &src.referenceable_param_group_ref,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(
                        TagId::CvParam,
                        component_id,
                        instrument_id,
                        &src.cv_param,
                    );
                }

                for analyzer in &component_list.analyzer {
                    let component_id = id_gen.alloc();
                    meta.touch_tagged_ids(TagId::ComponentAnalyzer, component_id, instrument_id);

                    meta.push_attr_usize_tagged_ids(
                        TagId::ComponentAnalyzer,
                        component_id,
                        instrument_id,
                        ACC_ATTR_ORDER,
                        analyzer.order,
                    );

                    meta.extend_ref_group_cv_params_ids(
                        TagId::ComponentAnalyzer,
                        component_id,
                        instrument_id,
                        &analyzer.referenceable_param_group_ref,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(
                        TagId::CvParam,
                        component_id,
                        instrument_id,
                        &analyzer.cv_param,
                    );
                }

                for detector in &component_list.detector {
                    let component_id = id_gen.alloc();
                    meta.touch_tagged_ids(TagId::ComponentDetector, component_id, instrument_id);

                    meta.push_attr_usize_tagged_ids(
                        TagId::ComponentDetector,
                        component_id,
                        instrument_id,
                        ACC_ATTR_ORDER,
                        detector.order,
                    );

                    meta.extend_ref_group_cv_params_ids(
                        TagId::ComponentDetector,
                        component_id,
                        instrument_id,
                        &detector.referenceable_param_group_ref,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(
                        TagId::CvParam,
                        component_id,
                        instrument_id,
                        &detector.cv_param,
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
    let n_instrument_configs = (items.len() - instr_start) as u32;

    let sw_start = items.len();
    if let Some(software_list) = &mzml.software_list {
        for software in &software_list.software {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let software_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::Software, software_id, 0);

            meta.push_attr_string_tagged_ids(
                TagId::Software,
                software_id,
                0,
                ACC_ATTR_ID,
                software.id.as_str(),
            );

            let ver = software.version.as_deref().or_else(|| {
                software
                    .software_param
                    .first()
                    .and_then(|p| p.version.as_deref())
            });
            if let Some(ver) = ver {
                meta.push_attr_string_tagged_ids(
                    TagId::Software,
                    software_id,
                    0,
                    ACC_ATTR_VERSION,
                    ver,
                );
            }

            for software_param in &software.software_param {
                let software_param_id = id_gen.alloc();
                meta.touch_tagged_ids(TagId::SoftwareParam, software_param_id, software_id);

                meta.push_tagged_ids(
                    TagId::SoftwareParam,
                    software_param_id,
                    software_id,
                    CvParam {
                        cv_ref: software_param.cv_ref.clone(),
                        accession: Some(software_param.accession.clone()),
                        name: software_param.name.clone(),
                        value: Some(String::new()),
                        unit_cv_ref: None,
                        unit_name: None,
                        unit_accession: None,
                    },
                );
            }

            meta.extend_tagged_ids(TagId::CvParam, software_id, 0, &software.cv_param);

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
    if let Some(data_processing_list) = &mzml.data_processing_list {
        for data_processing in &data_processing_list.data_processing {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let data_processing_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::DataProcessing, data_processing_id, 0);

            meta.push_attr_string_tagged_ids(
                TagId::DataProcessing,
                data_processing_id,
                0,
                ACC_ATTR_ID,
                data_processing.id.as_str(),
            );

            for processing_method in &data_processing.processing_method {
                let processing_method_id = id_gen.alloc();
                meta.touch_tagged_ids(
                    TagId::ProcessingMethod,
                    processing_method_id,
                    data_processing_id,
                );

                meta.extend_ref_group_cv_params_ids(
                    TagId::ProcessingMethod,
                    processing_method_id,
                    data_processing_id,
                    &processing_method.referenceable_param_group_ref,
                    ref_groups,
                );

                meta.extend_tagged_ids(
                    TagId::CvParam,
                    processing_method_id,
                    data_processing_id,
                    &processing_method.cv_param,
                );
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
    if let Some(scan_settings_list) = &mzml.scan_settings_list {
        for scan_settings in &scan_settings_list.scan_settings {
            let mut out = Vec::new();
            let mut tags = Vec::new();
            let mut owners = Vec::new();
            let mut parents = Vec::new();
            let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

            let scan_settings_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::ScanSettings, scan_settings_id, 0);

            if let Some(id) = scan_settings.id.as_deref() {
                meta.push_attr_string_tagged_ids(
                    TagId::ScanSettings,
                    scan_settings_id,
                    0,
                    ACC_ATTR_ID,
                    id,
                );
            }

            if let Some(icr) = scan_settings.instrument_configuration_ref.as_deref() {
                meta.push_attr_string_tagged_ids(
                    TagId::ScanSettings,
                    scan_settings_id,
                    0,
                    ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
                    icr,
                );
            }

            if let Some(source_file_ref_list) = &scan_settings.source_file_ref_list {
                let source_file_ref_list_id = id_gen.alloc();
                meta.touch_tagged_ids(
                    TagId::SourceFileRefList,
                    source_file_ref_list_id,
                    scan_settings_id,
                );

                meta.push_attr_usize_tagged_ids(
                    TagId::SourceFileRefList,
                    source_file_ref_list_id,
                    scan_settings_id,
                    ACC_ATTR_COUNT,
                    Some(source_file_ref_list.source_file_refs.len() as u32),
                );

                for source_file_ref in &source_file_ref_list.source_file_refs {
                    let source_file_ref_id = id_gen.alloc();
                    meta.touch_tagged_ids(
                        TagId::SourceFileRef,
                        source_file_ref_id,
                        source_file_ref_list_id,
                    );

                    meta.push_attr_string_tagged_ids(
                        TagId::SourceFileRef,
                        source_file_ref_id,
                        source_file_ref_list_id,
                        ACC_ATTR_REF,
                        source_file_ref.r#ref.as_str(),
                    );
                }
            }

            meta.extend_ref_group_cv_params_ids(
                TagId::ScanSettings,
                scan_settings_id,
                0,
                &scan_settings.referenceable_param_group_refs,
                ref_groups,
            );

            meta.extend_tagged_ids(
                TagId::CvParam,
                scan_settings_id,
                0,
                &scan_settings.cv_params,
            );

            if let Some(target_list) = &scan_settings.target_list {
                for target in &target_list.targets {
                    let target_id = id_gen.alloc();
                    meta.touch_tagged_ids(TagId::Target, target_id, scan_settings_id);

                    meta.extend_ref_group_cv_params_ids(
                        TagId::Target,
                        target_id,
                        scan_settings_id,
                        &target.referenceable_param_group_refs,
                        ref_groups,
                    );

                    meta.extend_tagged_ids(
                        TagId::CvParam,
                        target_id,
                        scan_settings_id,
                        &target.cv_params,
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
    let n_acquisition_settings = (items.len() - acq_start) as u32;

    let cv_start = items.len();
    if let Some(cv_list) = &mzml.cv_list {
        let cv_count = cv_list.cv.len() as u32;

        if cv_count != 0 {
            let cv_list_id = id_gen.alloc();

            for (i, cv) in cv_list.cv.iter().enumerate() {
                let mut out = Vec::new();
                let mut tags = Vec::new();
                let mut owners = Vec::new();
                let mut parents = Vec::new();
                let mut meta = MetaAcc::new(&mut out, &mut tags, &mut owners, &mut parents);

                if i == 0 {
                    meta.touch_tagged_ids(TagId::CvList, cv_list_id, 0);
                    meta.push_attr_usize_tagged_ids(
                        TagId::CvList,
                        cv_list_id,
                        0,
                        ACC_ATTR_COUNT,
                        Some(cv_count),
                    );
                }

                let cv_id = id_gen.alloc();
                meta.touch_tagged_ids(TagId::Cv, cv_id, cv_list_id);

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
    record_id: u32,
    parent_record_id: u32,
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
    numeric_index: &mut u32,
    string_index: &mut u32,
) {
    tag_ids.push(tag_id);
    ids.push(record_id);
    parent_indices.push(parent_record_id);

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

fn pack_meta_streaming<T, F>(row_ids: &mut RowIdGen, items: &[T], mut fill: F) -> PackedMeta
where
    F: FnMut(&mut MetaAcc<'_>, &T),
{
    let item_count = items.len();

    let mut index_offsets = Vec::with_capacity(item_count + 1);

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
            let element_id = scratch_owners[i];
            let parent_element_id = scratch_parents[i];

            let _ = row_ids.alloc();

            pack_cv_param(
                scratch_tags[i],
                element_id,
                parent_element_id,
                &scratch[i],
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
                &mut numeric_index,
                &mut string_index,
            );
            meta_index += 1;
        }

        index_offsets.push(meta_index);
    }

    PackedMeta {
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

fn pack_meta_slices<T, F>(row_ids: &mut RowIdGen, items: &[T], meta_of: F) -> PackedMeta
where
    F: Fn(&T) -> (&[CvParam], &[u8], &[u32], &[u32]),
{
    let item_count = items.len();

    let mut total_meta_count = 0usize;
    for item in items {
        total_meta_count += meta_of(item).0.len();
    }

    let mut index_offsets = Vec::with_capacity(item_count + 1);

    let mut ids = Vec::with_capacity(total_meta_count);
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
            let element_id = os[i];
            let parent_element_id = ps[i];

            let _ = row_ids.alloc();

            pack_cv_param(
                ts[i],
                element_id,
                parent_element_id,
                &xs[i],
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
                &mut numeric_index,
                &mut string_index,
            );
            meta_index += 1;
        }

        index_offsets.push(meta_index);
    }

    PackedMeta {
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

fn packed_meta_byte_len(meta: &PackedMeta) -> usize {
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

fn write_packed_meta_into(buf: &mut Vec<u8>, meta: &PackedMeta) {
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

pub fn encode(mzml: &MzML, compression_level: u8, f32_compress: bool) -> Vec<u8> {
    assert!(compression_level <= 22);

    #[inline]
    fn fix_attr_values(out: &mut Vec<CvParam>) {
        for cv in out.iter_mut() {
            if cv.cv_ref.as_deref() == Some(CV_REF_ATTR) {
                let empty_val = cv.value.as_deref().map_or(true, |s| s.is_empty());
                if empty_val && !cv.name.is_empty() {
                    cv.value = Some(std::mem::take(&mut cv.name));
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
    let mut row_ids = RowIdGen::new();

    let spectrum_list_id: u32 = if run.spectrum_list.is_some() {
        id_gen.alloc()
    } else {
        0
    };
    let chromatogram_list_id: u32 = if run.chromatogram_list.is_some() {
        id_gen.alloc()
    } else {
        0
    };

    let (mut global_items, global_counts) = build_global_meta_items(mzml, &ref_groups, &mut id_gen);
    for item in &mut global_items {
        fix_attr_values(&mut item.cvs);
    }

    let mut spec_i: usize = 0;
    let spectrum_meta = pack_meta_streaming(&mut row_ids, spectra, |meta, s| {
        let idx = spec_i;
        spec_i += 1;

        if idx == 0 && spectrum_list_id != 0 {
            let sl = run.spectrum_list.as_ref().unwrap();
            meta.touch_tagged_ids(TagId::SpectrumList, spectrum_list_id, 0);
            meta.push_schema_attributes(TagId::SpectrumList, spectrum_list_id, 0, sl);
        }

        let spectrum_id = id_gen.alloc();

        meta.push_schema_attributes(TagId::Spectrum, spectrum_id, spectrum_list_id, s);

        if s.index.is_none() {
            meta.push_attr_usize_tagged_ids(
                TagId::Spectrum,
                spectrum_id,
                spectrum_list_id,
                ACC_ATTR_INDEX,
                Some(idx as u32),
            );
        }

        meta.extend_ref_group_cv_params_ids(
            TagId::Spectrum,
            spectrum_id,
            spectrum_list_id,
            &s.referenceable_param_group_refs,
            &ref_groups,
        );

        meta.extend_tagged_ids(TagId::CvParam, spectrum_id, spectrum_list_id, &s.cv_params);

        flatten_spectrum_metadata_into_owned(
            meta,
            s,
            &ref_groups,
            spectrum_id,
            spectrum_list_id,
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
    let chromatogram_meta = pack_meta_streaming(&mut row_ids, chromatograms, |meta, c| {
        let idx = chrom_i;
        chrom_i += 1;

        if idx == 0 && chromatogram_list_id != 0 {
            let cl = run.chromatogram_list.as_ref().unwrap();
            meta.touch_tagged_ids(TagId::ChromatogramList, chromatogram_list_id, 0);
            meta.push_schema_attributes(TagId::ChromatogramList, chromatogram_list_id, 0, cl);
        }

        let chrom_id = id_gen.alloc();

        meta.push_schema_attributes(TagId::Chromatogram, chrom_id, chromatogram_list_id, c);

        meta.extend_ref_group_cv_params_ids(
            TagId::Chromatogram,
            chrom_id,
            chromatogram_list_id,
            &c.referenceable_param_group_refs,
            &ref_groups,
        );

        meta.extend_tagged_ids(TagId::CvParam, chrom_id, chromatogram_list_id, &c.cv_params);

        flatten_chromatogram_metadata_into(
            meta,
            c,
            &ref_groups,
            chrom_id,
            chromatogram_list_id,
            &mut id_gen,
            ACC_TIME_ARRAY,
            ACC_INTENSITY_ARRAY,
            false,
            false,
            f32_compress,
        );

        fix_attr_values(meta.out);
    });

    let global_meta = pack_meta_slices(&mut row_ids, &global_items, |m| {
        (
            m.cvs.as_slice(),
            m.tags.as_slice(),
            m.owners.as_slice(),
            m.parents.as_slice(),
        )
    });

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

    let mut spec_entries_bytes = Vec::new();
    let mut spec_arrayrefs_bytes = Vec::new();
    let mut spect_array_types = HashSet::new();
    let mut spect_builder =
        ContainerBuilder::new(TARGET_BLOCK_UNCOMP_BYTES, compression_level, do_shuffle);
    let mut spec_a1_index: u64 = 0;

    for s in spectra {
        let arr_ref_start = spec_a1_index;
        let mut arr_ref_count: u64 = 0;
        if let Some(bal) = s.binary_data_array_list.as_ref() {
            for ba in &bal.binary_data_arrays {
                if let Some(arr) = array_ref(ba) {
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
                    let (block_id, element_off) =
                        spect_builder.add_item_to_box(arr.len() * esz, esz, |buf| {
                            write_array(buf, arr, writer_dtype)
                        });
                    write_u64_le(&mut spec_arrayrefs_bytes, element_off);
                    write_u64_le(&mut spec_arrayrefs_bytes, arr.len() as u64);
                    write_u32_le(&mut spec_arrayrefs_bytes, block_id);
                    write_u32_le(&mut spec_arrayrefs_bytes, array_type);
                    spec_arrayrefs_bytes.push(entry_dtype);
                    spec_arrayrefs_bytes.extend_from_slice(&[0u8; 7]);
                    spec_a1_index += 1;
                    arr_ref_count += 1;
                }
            }
        }
        write_u64_le(&mut spec_entries_bytes, arr_ref_start);
        write_u64_le(&mut spec_entries_bytes, arr_ref_count);
    }

    let mut chrom_entries_bytes = Vec::new();
    let mut chrom_arrayrefs_bytes = Vec::new();
    let mut chrom_array_types = HashSet::new();
    let mut chrom_builder =
        ContainerBuilder::new(TARGET_BLOCK_UNCOMP_BYTES, compression_level, do_shuffle);
    let mut chrom_b1_index: u64 = 0;

    for c in chromatograms {
        let arr_ref_start = chrom_b1_index;
        let mut arr_ref_count: u64 = 0;
        if let Some(bal) = c.binary_data_array_list.as_ref() {
            for ba in &bal.binary_data_arrays {
                if let Some(arr) = array_ref(ba) {
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
                    let (block_id, element_off) =
                        chrom_builder.add_item_to_box(arr.len() * esz, esz, |buf| {
                            write_array(buf, arr, writer_dtype)
                        });
                    write_u64_le(&mut chrom_arrayrefs_bytes, element_off);
                    write_u64_le(&mut chrom_arrayrefs_bytes, arr.len() as u64);
                    write_u32_le(&mut chrom_arrayrefs_bytes, block_id);
                    write_u32_le(&mut chrom_arrayrefs_bytes, array_type);
                    chrom_arrayrefs_bytes.push(entry_dtype);
                    chrom_arrayrefs_bytes.extend_from_slice(&[0u8; 7]);
                    chrom_b1_index += 1;
                    arr_ref_count += 1;
                }
            }
        }
        write_u64_le(&mut chrom_entries_bytes, arr_ref_start);
        write_u64_le(&mut chrom_entries_bytes, arr_ref_count);
    }

    let (container_spect, block_count_spect) = spect_builder.pack();
    let (container_chrom, block_count_chrom) = chrom_builder.pack();

    let mut output =
        Vec::with_capacity(HEADER_SIZE + container_spect.len() + container_chrom.len() + 1024);
    output.resize(HEADER_SIZE, 0);

    let off_spec_entries = append_aligned_8(&mut output, &spec_entries_bytes);
    let off_spec_arrayrefs = append_aligned_8(&mut output, &spec_arrayrefs_bytes);
    let off_chrom_entries = append_aligned_8(&mut output, &chrom_entries_bytes);
    let off_chrom_arrayrefs = append_aligned_8(&mut output, &chrom_arrayrefs_bytes);
    let off_spec_meta = append_aligned_8(&mut output, &spectrum_meta_bytes);
    let off_chrom_meta = append_aligned_8(&mut output, &chromatogram_meta_bytes);
    let off_global_meta = append_aligned_8(&mut output, &global_meta_bytes);
    let off_container_spect = append_aligned_8(&mut output, &container_spect);
    let off_container_chrom = append_aligned_8(&mut output, &container_chrom);

    {
        let header = &mut output[0..HEADER_SIZE];
        header[0..4].copy_from_slice(b"B000");
        set_u64_at(header, 8, off_spec_entries);
        set_u64_at(header, 16, spec_entries_bytes.len() as u64);
        set_u64_at(header, 24, off_spec_arrayrefs);
        set_u64_at(header, 32, spec_arrayrefs_bytes.len() as u64);
        set_u64_at(header, 40, off_chrom_entries);
        set_u64_at(header, 48, chrom_entries_bytes.len() as u64);
        set_u64_at(header, 56, off_chrom_arrayrefs);
        set_u64_at(header, 64, chrom_arrayrefs_bytes.len() as u64);
        set_u64_at(header, 72, off_spec_meta);
        set_u64_at(header, 80, spectrum_meta_bytes.len() as u64);
        set_u64_at(header, 88, off_chrom_meta);
        set_u64_at(header, 96, chromatogram_meta_bytes.len() as u64);
        set_u64_at(header, 104, off_global_meta);
        set_u64_at(header, 112, global_meta_bytes.len() as u64);
        set_u64_at(header, 120, off_container_spect);
        set_u64_at(header, 128, container_spect.len() as u64);
        set_u64_at(header, 136, off_container_chrom);
        set_u64_at(header, 144, container_chrom.len() as u64);
        set_u32_at(header, 152, block_count_spect);
        set_u32_at(header, 156, block_count_chrom);
        set_u32_at(header, 160, spectrum_count);
        set_u32_at(header, 164, chrom_count);
        set_u32_at(header, 168, spectrum_meta.ref_codes.len() as u32);
        set_u32_at(header, 172, spectrum_meta.numeric_values.len() as u32);
        set_u32_at(header, 176, spectrum_meta.string_offsets.len() as u32);
        set_u32_at(header, 180, chromatogram_meta.ref_codes.len() as u32);
        set_u32_at(header, 184, chromatogram_meta.numeric_values.len() as u32);
        set_u32_at(header, 188, chromatogram_meta.string_offsets.len() as u32);
        set_u32_at(header, 192, global_meta.ref_codes.len() as u32);
        set_u32_at(header, 196, global_meta.numeric_values.len() as u32);
        set_u32_at(header, 200, global_meta.string_offsets.len() as u32);
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

fn flatten_spectrum_metadata_into_owned(
    meta: &mut MetaAcc<'_>,
    spectrum: &Spectrum,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    spectrum_id: u32,
    parent_id: u32,
    id_gen: &mut NodeIdGen,
    x_tail: u32,
    y_tail: u32,
    _x_64: bool,
    _y_64: bool,
    f32_c: bool,
) {
    let _ = parent_id;

    if let Some(sd) = &spectrum.spectrum_description {
        let sd_id = id_gen.alloc();
        meta.touch_tagged_ids(TagId::SpectrumDescription, sd_id, spectrum_id);

        meta.extend_tagged_ids(TagId::CvParam, sd_id, spectrum_id, &sd.cv_params);

        if let Some(sl) = &sd.scan_list {
            let sl_id = id_gen.alloc();
            meta.push_attr_usize_tagged_ids(
                TagId::ScanList,
                sl_id,
                sd_id,
                ACC_ATTR_COUNT,
                Some(sl.scans.len() as u32),
            );
            flatten_scan_list_ids(meta, sl, sl_id, sd_id, id_gen, ref_groups);
        }
        if let Some(pl) = &sd.precursor_list {
            let pl_id = id_gen.alloc();
            meta.push_attr_usize_tagged_ids(
                TagId::PrecursorList,
                pl_id,
                sd_id,
                ACC_ATTR_COUNT,
                Some(pl.precursors.len() as u32),
            );
            for p in &pl.precursors {
                flatten_precursor_ids(meta, p, pl_id, id_gen, ref_groups);
            }
        }
        if let Some(pl) = &sd.product_list {
            let pl_id = id_gen.alloc();
            meta.push_attr_usize_tagged_ids(
                TagId::ProductList,
                pl_id,
                sd_id,
                ACC_ATTR_COUNT,
                Some(pl.products.len() as u32),
            );
            for p in &pl.products {
                flatten_product_ids(meta, p, pl_id, id_gen, ref_groups);
            }
        }
    }

    if let Some(sl) = &spectrum.scan_list {
        let sl_id = id_gen.alloc();
        meta.push_attr_usize_tagged_ids(
            TagId::ScanList,
            sl_id,
            spectrum_id,
            ACC_ATTR_COUNT,
            Some(sl.scans.len() as u32),
        );
        flatten_scan_list_ids(meta, sl, sl_id, spectrum_id, id_gen, ref_groups);
    }
    if let Some(pl) = &spectrum.precursor_list {
        let pl_id = id_gen.alloc();
        meta.push_attr_usize_tagged_ids(
            TagId::PrecursorList,
            pl_id,
            spectrum_id,
            ACC_ATTR_COUNT,
            Some(pl.precursors.len() as u32),
        );
        for p in &pl.precursors {
            flatten_precursor_ids(meta, p, pl_id, id_gen, ref_groups);
        }
    }
    if let Some(pl) = &spectrum.product_list {
        let pl_id = id_gen.alloc();
        meta.push_attr_usize_tagged_ids(
            TagId::ProductList,
            pl_id,
            spectrum_id,
            ACC_ATTR_COUNT,
            Some(pl.products.len() as u32),
        );
        for p in &pl.products {
            flatten_product_ids(meta, p, pl_id, id_gen, ref_groups);
        }
    }

    if let Some(bal) = &spectrum.binary_data_array_list {
        let bal_id = id_gen.alloc();
        meta.push_attr_usize_tagged_ids(
            TagId::BinaryDataArrayList,
            bal_id,
            spectrum_id,
            ACC_ATTR_COUNT,
            Some(bal.binary_data_arrays.len() as u32),
        );
        for ba in &bal.binary_data_arrays {
            let ba_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::BinaryDataArray, ba_id, bal_id);
            meta.push_schema_attributes(TagId::BinaryDataArray, ba_id, bal_id, ba);
            extend_binary_data_array_cv_params_ids(meta, ba_id, bal_id, ba, x_tail, y_tail, f32_c);
        }
    }
}

fn flatten_chromatogram_metadata_into(
    meta: &mut MetaAcc<'_>,
    chrom: &Chromatogram,
    ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    chrom_id: u32,
    parent_id: u32,
    id_gen: &mut NodeIdGen,
    x_tail: u32,
    y_tail: u32,
    _x_64: bool,
    _y_64: bool,
    f32_c: bool,
) {
    let _ = parent_id;

    if let Some(p) = &chrom.precursor {
        flatten_precursor_ids(meta, p, chrom_id, id_gen, ref_groups);
    }
    if let Some(p) = &chrom.product {
        flatten_product_ids(meta, p, chrom_id, id_gen, ref_groups);
    }

    if let Some(bal) = &chrom.binary_data_array_list {
        let bal_id = id_gen.alloc();
        meta.push_attr_usize_tagged_ids(
            TagId::BinaryDataArrayList,
            bal_id,
            chrom_id,
            ACC_ATTR_COUNT,
            Some(bal.binary_data_arrays.len() as u32),
        );
        for ba in &bal.binary_data_arrays {
            let ba_id = id_gen.alloc();
            meta.touch_tagged_ids(TagId::BinaryDataArray, ba_id, bal_id);
            meta.push_schema_attributes(TagId::BinaryDataArray, ba_id, bal_id, ba);
            extend_binary_data_array_cv_params_ids(meta, ba_id, bal_id, ba, x_tail, y_tail, f32_c);
        }
    }
}

fn flatten_precursor_ids(
    meta: &mut MetaAcc<'_>,
    p: &Precursor,
    parent: u32,
    id_gen: &mut NodeIdGen,
    _groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    let pid = id_gen.alloc();
    meta.touch_tagged_ids(TagId::Precursor, pid, parent);
    meta.push_schema_attributes(TagId::Precursor, pid, parent, p);

    if let Some(iw) = &p.isolation_window {
        let id = id_gen.alloc();
        meta.touch_tagged_ids(TagId::IsolationWindow, id, pid);
        meta.extend_tagged_ids(TagId::CvParam, id, pid, &iw.cv_params);
    }
    if let Some(sil) = &p.selected_ion_list {
        let sid = id_gen.alloc();
        meta.push_attr_usize_tagged_ids(
            TagId::SelectedIonList,
            sid,
            pid,
            ACC_ATTR_COUNT,
            Some(sil.selected_ions.len() as u32),
        );
        for ion in &sil.selected_ions {
            let iid = id_gen.alloc();
            meta.touch_tagged_ids(TagId::SelectedIon, iid, sid);
            meta.push_schema_attributes(TagId::SelectedIon, iid, sid, ion);
            meta.extend_tagged_ids(TagId::CvParam, iid, sid, &ion.cv_params);
        }
    }
    if let Some(act) = &p.activation {
        let aid = id_gen.alloc();
        meta.touch_tagged_ids(TagId::Activation, aid, pid);
        meta.extend_tagged_ids(TagId::CvParam, aid, pid, &act.cv_params);
    }
}

fn flatten_product_ids(
    meta: &mut MetaAcc<'_>,
    p: &Product,
    parent: u32,
    id_gen: &mut NodeIdGen,
    _groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    let pid = id_gen.alloc();
    meta.touch_tagged_ids(TagId::Product, pid, parent);
    meta.push_schema_attributes(TagId::Product, pid, parent, p);
    if let Some(iw) = &p.isolation_window {
        let id = id_gen.alloc();
        meta.touch_tagged_ids(TagId::IsolationWindow, id, pid);
        meta.extend_tagged_ids(TagId::CvParam, id, pid, &iw.cv_params);
    }
}

fn flatten_scan_list_ids(
    meta: &mut MetaAcc<'_>,
    sl: &ScanList,
    lid: u32,
    lid_parent: u32,
    id_gen: &mut NodeIdGen,
    _groups: &HashMap<&str, &ReferenceableParamGroup>,
) {
    let _ = lid_parent;

    for scan in &sl.scans {
        let sid = id_gen.alloc();
        meta.touch_tagged_ids(TagId::Scan, sid, lid);
        meta.push_schema_attributes(TagId::Scan, sid, lid, scan);
        meta.extend_tagged_ids(TagId::CvParam, sid, lid, &scan.cv_params);

        if let Some(wl) = &scan.scan_window_list {
            let wid = id_gen.alloc();
            meta.push_attr_usize_tagged_ids(
                TagId::ScanWindowList,
                wid,
                sid,
                ACC_ATTR_COUNT,
                Some(wl.scan_windows.len() as u32),
            );
            for w in &wl.scan_windows {
                let id = id_gen.alloc();
                meta.touch_tagged_ids(TagId::ScanWindow, id, wid);
                meta.push_schema_attributes(TagId::ScanWindow, id, wid, w);
                meta.extend_tagged_ids(TagId::CvParam, id, wid, &w.cv_params);
            }
        }
    }
}
