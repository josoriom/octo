use serde::Serialize;
use std::{collections::HashMap, slice};
use zstd::bulk::compress as zstd_compress;

use crate::{
    UserParam,
    b64::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_CV_FULL_NAME, ACC_ATTR_CV_URI, ACC_ATTR_CV_VERSION,
            ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_DEFAULT_SOURCE_FILE_REF,
            ACC_ATTR_ID, ACC_ATTR_INDEX, ACC_ATTR_INSTRUMENT_CONFIGURATION_REF, ACC_ATTR_LABEL,
            ACC_ATTR_LOCATION, ACC_ATTR_NAME, ACC_ATTR_ORDER, ACC_ATTR_REF, ACC_ATTR_SAMPLE_REF,
            ACC_ATTR_START_TIME_STAMP, ACC_ATTR_VERSION, AccessionTail, CV_REF_ATTR, attr_cv_param,
            cv_ref_code_from_str, parse_accession_tail,
        },
        utilities::assign_attributes,
    },
    decoder::decode::MetadatumValue,
    encoder::utilities::le_writers::{write_f64_slice_le, write_u32_le, write_u32_slice_le},
    mzml::{
        schema::TagId,
        structs::{
            BinaryDataArray, BinaryDataArrayList, Chromatogram, CvParam, MzML, Precursor, Product,
            ReferenceableParamGroup, ReferenceableParamGroupRef, ScanList, Spectrum,
            SpectrumDescription,
        },
    },
};

pub(crate) const ACCESSION_MZ_ARRAY: u32 = 1_000_514;
pub(crate) const ACCESSION_INTENSITY_ARRAY: u32 = 1_000_515;
pub(crate) const ACCESSION_TIME_ARRAY: u32 = 1_000_595;
pub(crate) const ACCESSION_32BIT_FLOAT: u32 = 1_000_521;
pub(crate) const ACCESSION_64BIT_FLOAT: u32 = 1_000_523;

const USER_PARAM_NAME_VALUE_SEPARATOR: char = '\0';

pub(crate) struct MetaCollector<'m> {
    ctx: TraversalCtx<'m>,
}

impl<'m> MetaCollector<'m> {
    pub(crate) fn new(ref_groups: &'m HashMap<&'m str, &'m ReferenceableParamGroup>) -> Self {
        Self {
            ctx: TraversalCtx::new(ref_groups),
        }
    }

    #[inline]
    pub(crate) fn alloc(&mut self) -> u32 {
        self.ctx.alloc()
    }

    pub(crate) fn collect_item_list_meta<T, L>(
        &mut self,
        items: &[T],
        list_node_id: u32,
        list_schema: Option<&L>,
        policy: ArrayPolicy,
    ) -> PackedMeta
    where
        T: MzmlListItem,
        L: Serialize,
    {
        pack_item_list_meta(items, list_node_id, list_schema, &mut self.ctx, policy)
    }

    pub(crate) fn collect_global_meta(&mut self, mzml: &MzML) -> (PackedMeta, GlobalCounts) {
        pack_global_meta(mzml, &mut self.ctx)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ArrayPolicy {
    pub(crate) x_array_accession: u32,
    pub(crate) y_array_accession: u32,
    pub(crate) force_f32: bool,
}

impl ArrayPolicy {
    pub(crate) fn is_xy_array(self, accession: u32) -> bool {
        accession == self.x_array_accession || accession == self.y_array_accession
    }
    pub(crate) fn should_force_f32(self, accession: u32) -> bool {
        self.force_f32 && self.is_xy_array(accession)
    }
}

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
pub(crate) struct GlobalCounts {
    pub(crate) n_file_description: u32,
    pub(crate) n_run: u32,
    pub(crate) n_ref_param_groups: u32,
    pub(crate) n_samples: u32,
    pub(crate) n_instrument_configs: u32,
    pub(crate) n_software: u32,
    pub(crate) n_data_processing: u32,
    pub(crate) n_acquisition_settings: u32,
    pub(crate) n_cvs: u32,
}

pub(crate) struct CompressedMetaSections {
    pub(crate) spectrum_bytes: Vec<u8>,
    pub(crate) chromatogram_bytes: Vec<u8>,
    pub(crate) global_bytes: Vec<u8>,
    pub(crate) spectrum_uncompressed_size: u64,
    pub(crate) chromatogram_uncompressed_size: u64,
    pub(crate) global_uncompressed_size: u64,
}

impl CompressedMetaSections {
    pub(crate) fn build(
        spectrum_meta: &PackedMeta,
        chrom_meta: &PackedMeta,
        global_meta: &PackedMeta,
        counts: &GlobalCounts,
        level: u8,
    ) -> Self {
        let raw_s = serialize_packed_meta(spectrum_meta);
        let raw_c = serialize_packed_meta(chrom_meta);
        let raw_g = serialize_global_meta_with_counts(counts, global_meta);
        Self {
            spectrum_uncompressed_size: raw_s.len() as u64,
            chromatogram_uncompressed_size: raw_c.len() as u64,
            global_uncompressed_size: raw_g.len() as u64,
            spectrum_bytes: compress_bytes_if_enabled(raw_s, level),
            chromatogram_bytes: compress_bytes_if_enabled(raw_c, level),
            global_bytes: compress_bytes_if_enabled(raw_g, level),
        }
    }
}

struct IdAllocator(u32);

impl IdAllocator {
    fn new() -> Self {
        Self(1)
    }
    #[inline]
    fn next(&mut self) -> u32 {
        let id = self.0;
        self.0 += 1;
        id
    }
}

pub(crate) struct TraversalCtx<'l> {
    nodes: IdAllocator,
    ref_groups: &'l HashMap<&'l str, &'l ReferenceableParamGroup>,
}

impl<'l> TraversalCtx<'l> {
    fn new(ref_groups: &'l HashMap<&'l str, &'l ReferenceableParamGroup>) -> Self {
        Self {
            nodes: IdAllocator::new(),
            ref_groups,
        }
    }
    #[inline]
    fn alloc(&mut self) -> u32 {
        self.nodes.next()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ValueEncoding {
    kind: u8,
    index: u32,
}

struct ValuePool {
    numeric_values: Vec<f64>,
    string_offsets: Vec<u32>,
    string_lengths: Vec<u32>,
    string_bytes: Vec<u8>,
    numeric_count: u32,
    string_count: u32,
}

impl ValuePool {
    fn new() -> Self {
        Self {
            numeric_values: Vec::new(),
            string_offsets: Vec::new(),
            string_lengths: Vec::new(),
            string_bytes: Vec::new(),
            numeric_count: 0,
            string_count: 0,
        }
    }

    fn encode(&mut self, value: Option<&str>) -> ValueEncoding {
        match value {
            None | Some("") => ValueEncoding { kind: 2, index: 0 },
            Some(text) => {
                if let Ok(n) = text.parse::<f64>() {
                    let index = self.numeric_count;
                    self.numeric_values.push(n);
                    self.numeric_count += 1;
                    ValueEncoding { kind: 0, index }
                } else {
                    let index = self.string_count;
                    let bytes = text.as_bytes();
                    self.string_offsets.push(self.string_bytes.len() as u32);
                    self.string_lengths.push(bytes.len() as u32);
                    self.string_bytes.extend_from_slice(bytes);
                    self.string_count += 1;
                    ValueEncoding { kind: 1, index }
                }
            }
        }
    }
}

struct MetaParamRow {
    cv_param: CvParam,
    tag_id: u8,
    owner_id: u32,
    parent_id: u32,
}

struct MetaParamBuffer {
    rows: Vec<MetaParamRow>,
}

impl MetaParamBuffer {
    fn new() -> Self {
        Self {
            rows: Vec::with_capacity(64),
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
    }

    fn push(&mut self, tag: TagId, owner_id: u32, parent_id: u32, cv_param: CvParam) {
        self.rows.push(MetaParamRow {
            cv_param,
            tag_id: tag as u8,
            owner_id,
            parent_id,
        });
    }

    fn extend_cv_params(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_id: u32,
        cv_params: &[CvParam],
    ) {
        self.rows.reserve(cv_params.len());
        for cv_param in cv_params {
            self.rows.push(MetaParamRow {
                cv_param: cv_param.clone(),
                tag_id: tag as u8,
                owner_id,
                parent_id,
            });
        }
    }

    fn extend_user_params(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_id: u32,
        user_params: &[UserParam],
    ) {
        self.rows.reserve(user_params.len());
        for user_param in user_params {
            self.rows.push(MetaParamRow {
                cv_param: encode_user_param_as_cv(user_param),
                tag_id: tag as u8,
                owner_id,
                parent_id,
            });
        }
    }

    fn as_writer(&mut self) -> MetaParamWriter<'_> {
        MetaParamWriter { buffer: self }
    }

    fn normalize_attr_cv_values(&mut self) {
        for row in &mut self.rows {
            if row.cv_param.cv_ref.as_deref() == Some(CV_REF_ATTR) {
                let absent = row.cv_param.value.as_deref().map_or(true, str::is_empty);
                if absent && !row.cv_param.name.is_empty() {
                    row.cv_param.value = Some(std::mem::take(&mut row.cv_param.name));
                }
            }
        }
    }
}

pub(crate) struct MetaParamWriter<'b> {
    buffer: &'b mut MetaParamBuffer,
}

impl<'b> MetaParamWriter<'b> {
    fn push_one(&mut self, tag: TagId, owner_id: u32, parent_id: u32, cv_param: CvParam) {
        self.buffer.push(tag, owner_id, parent_id, cv_param);
    }
    fn push_many(&mut self, tag: TagId, owner_id: u32, parent_id: u32, cv_params: &[CvParam]) {
        self.buffer
            .extend_cv_params(tag, owner_id, parent_id, cv_params);
    }
    fn push_user_params(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_id: u32,
        user_params: &[UserParam],
    ) {
        self.buffer
            .extend_user_params(tag, owner_id, parent_id, user_params);
    }
    fn touch(&mut self, tag: TagId, owner_id: u32, parent_id: u32) {
        self.push_one(tag, owner_id, parent_id, empty_cv_param());
    }
    fn push_str_attr(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_id: u32,
        tail: AccessionTail,
        value: &str,
    ) {
        if !value.is_empty() {
            self.push_one(tag, owner_id, parent_id, attr_cv_param(tail, value));
        }
    }
    fn push_optional_u32_attr(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_id: u32,
        tail: AccessionTail,
        value: Option<u32>,
    ) {
        if let Some(n) = value {
            self.push_one(
                tag,
                owner_id,
                parent_id,
                attr_cv_param(tail, &n.to_string()),
            );
        }
    }
    fn push_cv_and_user_params(
        &mut self,
        owner_id: u32,
        parent_id: u32,
        cv_params: &[CvParam],
        user_params: &[UserParam],
    ) {
        self.push_many(TagId::CvParam, owner_id, parent_id, cv_params);
        self.push_user_params(TagId::UserParam, owner_id, parent_id, user_params);
    }
    fn push_ref_group_params(
        &mut self,
        owner_id: u32,
        parent_id: u32,
        group_refs: &[ReferenceableParamGroupRef],
        ref_groups: &HashMap<&str, &ReferenceableParamGroup>,
    ) {
        for gr in group_refs {
            if let Some(group) = ref_groups.get(gr.r#ref.as_str()) {
                self.push_many(TagId::CvParam, owner_id, parent_id, &group.cv_params);
            }
        }
    }
    fn push_schema_attrs<T: Serialize>(
        &mut self,
        tag: TagId,
        owner_id: u32,
        parent_id: u32,
        schema_value: &T,
    ) {
        for attr in assign_attributes(schema_value, tag, owner_id, parent_id) {
            let tail_raw = parse_accession_tail_raw(attr.accession.as_deref());
            if tail_raw == 0 {
                continue;
            }
            let text = match attr.value {
                MetadatumValue::Text(t) => t,
                MetadatumValue::Number(n) => n.to_string(),
                MetadatumValue::Empty => continue,
            };
            if text.is_empty() {
                continue;
            }
            self.push_one(
                tag,
                owner_id,
                parent_id,
                attr_cv_param(AccessionTail::from_raw(tail_raw), &text),
            );
        }
    }
}

struct PackedMetaBuilder {
    index_offsets: Vec<u32>,
    ids: Vec<u32>,
    parent_indices: Vec<u32>,
    tag_ids: Vec<u8>,
    ref_codes: Vec<u8>,
    accession_numbers: Vec<u32>,
    unit_ref_codes: Vec<u8>,
    unit_accession_numbers: Vec<u32>,
    value_kinds: Vec<u8>,
    value_indices: Vec<u32>,
    value_pool: ValuePool,
    row_count: u32,
}

impl PackedMetaBuilder {
    fn new() -> Self {
        let mut b = Self {
            index_offsets: Vec::new(),
            ids: Vec::new(),
            parent_indices: Vec::new(),
            tag_ids: Vec::new(),
            ref_codes: Vec::new(),
            accession_numbers: Vec::new(),
            unit_ref_codes: Vec::new(),
            unit_accession_numbers: Vec::new(),
            value_kinds: Vec::new(),
            value_indices: Vec::new(),
            value_pool: ValuePool::new(),
            row_count: 0,
        };
        b.index_offsets.push(0);
        b
    }

    fn flush_buffer(&mut self, buffer: &MetaParamBuffer) {
        for row in &buffer.rows {
            self.push_row(row.tag_id, row.owner_id, row.parent_id, &row.cv_param);
        }
        self.end_item();
    }

    fn push_row(&mut self, tag_id: u8, owner_id: u32, parent_id: u32, cv_param: &CvParam) {
        self.tag_ids.push(tag_id);
        self.ids.push(owner_id);
        self.parent_indices.push(parent_id);

        let cv_ref = cv_ref_prefix_from_accession(cv_param.accession.as_deref())
            .or(cv_param.cv_ref.as_deref());
        self.ref_codes.push(cv_ref_code_from_str(cv_ref));
        self.accession_numbers
            .push(parse_accession_tail_raw(cv_param.accession.as_deref()));

        let unit_ref = cv_ref_prefix_from_accession(cv_param.unit_accession.as_deref())
            .or(cv_param.unit_cv_ref.as_deref());
        self.unit_ref_codes.push(cv_ref_code_from_str(unit_ref));
        self.unit_accession_numbers
            .push(parse_accession_tail_raw(cv_param.unit_accession.as_deref()));

        let enc = self.value_pool.encode(cv_param.value.as_deref());
        self.value_kinds.push(enc.kind);
        self.value_indices.push(enc.index);
        self.row_count += 1;
    }

    fn end_item(&mut self) {
        self.index_offsets.push(self.row_count);
    }

    fn build(self) -> PackedMeta {
        PackedMeta {
            index_offsets: self.index_offsets,
            ids: self.ids,
            parent_indices: self.parent_indices,
            tag_ids: self.tag_ids,
            ref_codes: self.ref_codes,
            accession_numbers: self.accession_numbers,
            unit_ref_codes: self.unit_ref_codes,
            unit_accession_numbers: self.unit_accession_numbers,
            value_kinds: self.value_kinds,
            value_indices: self.value_indices,
            numeric_values: self.value_pool.numeric_values,
            string_offsets: self.value_pool.string_offsets,
            string_lengths: self.value_pool.string_lengths,
            string_bytes: self.value_pool.string_bytes,
        }
    }
}

fn empty_cv_param() -> CvParam {
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

fn encode_user_param_as_cv(p: &UserParam) -> CvParam {
    let encoded = match &p.value {
        Some(v) => format!("{}{USER_PARAM_NAME_VALUE_SEPARATOR}{v}", p.name),
        None => format!("{}{USER_PARAM_NAME_VALUE_SEPARATOR}", p.name),
    };
    CvParam {
        cv_ref: None,
        accession: None,
        name: String::new(),
        value: Some(encoded),
        unit_cv_ref: p.unit_cv_ref.clone(),
        unit_name: p.unit_name.clone(),
        unit_accession: p.unit_accession.clone(),
    }
}

fn make_float_precision_cv_param(accession_tail: u32) -> CvParam {
    let name = if accession_tail == ACCESSION_32BIT_FLOAT {
        "32-bit float"
    } else {
        "64-bit float"
    };
    CvParam {
        cv_ref: Some("MS".to_string()),
        accession: Some(format!("MS:{accession_tail:07}")),
        name: name.to_string(),
        value: None,
        unit_cv_ref: None,
        unit_name: None,
        unit_accession: None,
    }
}

#[inline]
pub(crate) fn parse_accession_tail_raw(accession: Option<&str>) -> u32 {
    parse_accession_tail(accession).raw()
}

fn cv_ref_prefix_from_accession(accession: Option<&str>) -> Option<&str> {
    accession.and_then(|s| s.split_once(':').map(|(prefix, _)| prefix))
}

pub(crate) fn array_type_accession_from_binary_data_array(bda: &BinaryDataArray) -> u32 {
    for cv in &bda.cv_params {
        let t = parse_accession_tail_raw(cv.accession.as_deref());
        if matches!(
            t,
            ACCESSION_MZ_ARRAY | ACCESSION_INTENSITY_ARRAY | ACCESSION_TIME_ARRAY
        ) {
            return t;
        }
    }
    for cv in &bda.cv_params {
        let t = parse_accession_tail_raw(cv.accession.as_deref());
        if t != 0 && cv.name.to_ascii_lowercase().contains(" array") {
            return t;
        }
    }
    0
}

pub(crate) fn build_ref_group_lookup<'m>(
    mzml: &'m MzML,
) -> HashMap<&'m str, &'m ReferenceableParamGroup> {
    mzml.referenceable_param_group_list
        .as_ref()
        .map_or_else(HashMap::new, |list| {
            list.referenceable_param_groups
                .iter()
                .map(|g| (g.id.as_str(), g))
                .collect()
        })
}

fn emit_binary_data_array_cv_params(
    writer: &mut MetaParamWriter<'_>,
    bda_node_id: u32,
    bda_list_node_id: u32,
    bda: &BinaryDataArray,
    policy: ArrayPolicy,
) {
    let array_acc = array_type_accession_from_binary_data_array(bda);
    if !(policy.force_f32 && policy.is_xy_array(array_acc)) {
        writer.push_many(
            TagId::CvParam,
            bda_node_id,
            bda_list_node_id,
            &bda.cv_params,
        );
        return;
    }
    let mut precision_written = false;
    for cv in &bda.cv_params {
        let tail = parse_accession_tail_raw(cv.accession.as_deref());
        if tail == ACCESSION_32BIT_FLOAT || tail == ACCESSION_64BIT_FLOAT {
            if !precision_written {
                writer.push_one(
                    TagId::CvParam,
                    bda_node_id,
                    bda_list_node_id,
                    make_float_precision_cv_param(ACCESSION_32BIT_FLOAT),
                );
                precision_written = true;
            }
        } else {
            writer.push_one(TagId::CvParam, bda_node_id, bda_list_node_id, cv.clone());
        }
    }
    if !precision_written {
        writer.push_one(
            TagId::CvParam,
            bda_node_id,
            bda_list_node_id,
            make_float_precision_cv_param(ACCESSION_32BIT_FLOAT),
        );
    }
}

fn packed_meta_byte_size(m: &PackedMeta) -> usize {
    m.index_offsets.len() * 4
        + m.ids.len() * 4
        + m.parent_indices.len() * 4
        + m.tag_ids.len()
        + m.ref_codes.len()
        + m.accession_numbers.len() * 4
        + m.unit_ref_codes.len()
        + m.unit_accession_numbers.len() * 4
        + m.value_kinds.len()
        + m.value_indices.len() * 4
        + m.numeric_values.len() * 8
        + m.string_offsets.len() * 4
        + m.string_lengths.len() * 4
        + m.string_bytes.len()
}

fn write_packed_meta(buf: &mut Vec<u8>, m: &PackedMeta) {
    write_u32_slice_le(buf, &m.index_offsets);
    write_u32_slice_le(buf, &m.ids);
    write_u32_slice_le(buf, &m.parent_indices);
    buf.extend_from_slice(&m.tag_ids);
    buf.extend_from_slice(&m.ref_codes);
    write_u32_slice_le(buf, &m.accession_numbers);
    buf.extend_from_slice(&m.unit_ref_codes);
    write_u32_slice_le(buf, &m.unit_accession_numbers);
    buf.extend_from_slice(&m.value_kinds);
    write_u32_slice_le(buf, &m.value_indices);
    write_f64_slice_le(buf, &m.numeric_values);
    write_u32_slice_le(buf, &m.string_offsets);
    write_u32_slice_le(buf, &m.string_lengths);
    buf.extend_from_slice(&m.string_bytes);
}

fn serialize_packed_meta(m: &PackedMeta) -> Vec<u8> {
    let mut buf = Vec::with_capacity(packed_meta_byte_size(m));
    write_packed_meta(&mut buf, m);
    buf
}

fn serialize_global_meta_with_counts(counts: &GlobalCounts, m: &PackedMeta) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 * 4 + packed_meta_byte_size(m));
    for n in [
        counts.n_file_description,
        counts.n_run,
        counts.n_ref_param_groups,
        counts.n_samples,
        counts.n_instrument_configs,
        counts.n_software,
        counts.n_data_processing,
        counts.n_acquisition_settings,
        counts.n_cvs,
    ] {
        write_u32_le(&mut buf, n);
    }
    write_packed_meta(&mut buf, m);
    buf
}

fn compress_bytes_if_enabled(bytes: Vec<u8>, level: u8) -> Vec<u8> {
    if level == 0 {
        bytes
    } else {
        zstd_compress(&bytes, level as i32).expect("zstd compression failed")
    }
}

fn pack_meta_for_each<F: FnMut(&mut MetaParamWriter<'_>, usize)>(
    item_count: usize,
    mut fill: F,
) -> PackedMeta {
    let mut builder = PackedMetaBuilder::new();
    let mut buffer = MetaParamBuffer::new();
    for i in 0..item_count {
        buffer.clear();
        fill(&mut buffer.as_writer(), i);
        buffer.normalize_attr_cv_values();
        builder.flush_buffer(&buffer);
    }
    builder.build()
}

fn append_meta_buffer(
    buffers: &mut Vec<MetaParamBuffer>,
    fill: impl FnOnce(&mut MetaParamWriter<'_>),
) {
    let mut buffer = MetaParamBuffer::new();
    fill(&mut buffer.as_writer());
    buffer.normalize_attr_cv_values();
    buffers.push(buffer);
}

pub(crate) trait MzmlListItem: Serialize {
    fn list_tag() -> TagId;
    fn item_tag() -> TagId;
    fn has_explicit_index(&self) -> bool;
    fn cv_params(&self) -> &[CvParam];
    fn user_params(&self) -> &[UserParam];
    fn group_refs(&self) -> &[ReferenceableParamGroupRef];
    fn flatten_children(
        &self,
        writer: &mut MetaParamWriter<'_>,
        item_id: u32,
        ctx: &mut TraversalCtx<'_>,
        policy: ArrayPolicy,
    );
}

impl MzmlListItem for Spectrum {
    fn list_tag() -> TagId {
        TagId::SpectrumList
    }
    fn item_tag() -> TagId {
        TagId::Spectrum
    }
    fn has_explicit_index(&self) -> bool {
        self.index.is_some()
    }
    fn cv_params(&self) -> &[CvParam] {
        &self.cv_params
    }
    fn user_params(&self) -> &[UserParam] {
        &self.user_params
    }
    fn group_refs(&self) -> &[ReferenceableParamGroupRef] {
        &self.referenceable_param_group_refs
    }
    fn flatten_children(
        &self,
        writer: &mut MetaParamWriter<'_>,
        id: u32,
        ctx: &mut TraversalCtx<'_>,
        policy: ArrayPolicy,
    ) {
        flatten_spectrum_children(writer, self, id, ctx, policy);
    }
}

impl MzmlListItem for Chromatogram {
    fn list_tag() -> TagId {
        TagId::ChromatogramList
    }
    fn item_tag() -> TagId {
        TagId::Chromatogram
    }
    fn has_explicit_index(&self) -> bool {
        true
    }
    fn cv_params(&self) -> &[CvParam] {
        &self.cv_params
    }
    fn user_params(&self) -> &[UserParam] {
        &self.user_params
    }
    fn group_refs(&self) -> &[ReferenceableParamGroupRef] {
        &self.referenceable_param_group_refs
    }
    fn flatten_children(
        &self,
        writer: &mut MetaParamWriter<'_>,
        id: u32,
        ctx: &mut TraversalCtx<'_>,
        policy: ArrayPolicy,
    ) {
        flatten_chromatogram_children(writer, self, id, ctx, policy);
    }
}

fn pack_item_list_meta<T, L>(
    items: &[T],
    list_node_id: u32,
    list_schema: Option<&L>,
    ctx: &mut TraversalCtx<'_>,
    policy: ArrayPolicy,
) -> PackedMeta
where
    T: MzmlListItem,
    L: Serialize,
{
    pack_meta_for_each(items.len(), |writer, i| {
        let item = &items[i];
        if i == 0 && list_node_id != 0 {
            if let Some(schema) = list_schema {
                writer.touch(T::list_tag(), list_node_id, 0);
                writer.push_schema_attrs(T::list_tag(), list_node_id, 0, schema);
            }
        }
        let item_id = ctx.alloc();
        writer.push_schema_attrs(T::item_tag(), item_id, list_node_id, item);
        if !item.has_explicit_index() {
            writer.push_optional_u32_attr(
                T::item_tag(),
                item_id,
                list_node_id,
                ACC_ATTR_INDEX,
                Some(i as u32),
            );
        }
        writer.push_ref_group_params(item_id, list_node_id, item.group_refs(), ctx.ref_groups);
        writer.push_cv_and_user_params(item_id, list_node_id, item.cv_params(), item.user_params());
        item.flatten_children(writer, item_id, ctx, policy);
    })
}

fn flatten_spectrum_children(
    writer: &mut MetaParamWriter<'_>,
    spectrum: &Spectrum,
    spectrum_id: u32,
    ctx: &mut TraversalCtx<'_>,
    policy: ArrayPolicy,
) {
    if let Some(desc) = &spectrum.spectrum_description {
        flatten_legacy_spectrum_description(writer, desc, spectrum_id, ctx);
    }
    flatten_scan_list_opt(writer, spectrum.scan_list.as_ref(), spectrum_id, ctx);
    flatten_precursor_list_opt(writer, spectrum.precursor_list.as_ref(), spectrum_id, ctx);
    flatten_product_list_opt(writer, spectrum.product_list.as_ref(), spectrum_id, ctx);
    flatten_binary_data_array_list(
        writer,
        spectrum.binary_data_array_list.as_ref(),
        spectrum_id,
        ctx,
        policy,
    );
}

fn flatten_legacy_spectrum_description(
    writer: &mut MetaParamWriter<'_>,
    desc: &SpectrumDescription,
    spectrum_id: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    let desc_id = ctx.alloc();
    writer.touch(TagId::SpectrumDescription, desc_id, spectrum_id);
    writer.push_ref_group_params(
        desc_id,
        spectrum_id,
        &desc.referenceable_param_group_refs,
        ctx.ref_groups,
    );
    writer.push_cv_and_user_params(desc_id, spectrum_id, &desc.cv_params, &desc.user_params);
    flatten_scan_list_opt(writer, desc.scan_list.as_ref(), desc_id, ctx);
    flatten_precursor_list_opt(writer, desc.precursor_list.as_ref(), desc_id, ctx);
    flatten_product_list_opt(writer, desc.product_list.as_ref(), desc_id, ctx);
}

fn flatten_chromatogram_children(
    writer: &mut MetaParamWriter<'_>,
    chrom: &Chromatogram,
    chrom_id: u32,
    ctx: &mut TraversalCtx<'_>,
    policy: ArrayPolicy,
) {
    if let Some(p) = &chrom.precursor {
        flatten_precursor(writer, p, chrom_id, ctx);
    }
    if let Some(p) = &chrom.product {
        flatten_product(writer, p, chrom_id, ctx);
    }
    flatten_binary_data_array_list(
        writer,
        chrom.binary_data_array_list.as_ref(),
        chrom_id,
        ctx,
        policy,
    );
}

fn flatten_scan_list_opt(
    writer: &mut MetaParamWriter<'_>,
    sl: Option<&ScanList>,
    parent: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    let Some(sl) = sl else { return };
    let sl_id = ctx.alloc();
    writer.push_optional_u32_attr(
        TagId::ScanList,
        sl_id,
        parent,
        ACC_ATTR_COUNT,
        Some(sl.scans.len() as u32),
    );
    writer.push_cv_and_user_params(sl_id, parent, &sl.cv_params, &sl.user_params);
    flatten_scan_list(writer, sl, sl_id, ctx);
}

fn flatten_precursor_list_opt(
    writer: &mut MetaParamWriter<'_>,
    pl: Option<&crate::mzml::structs::PrecursorList>,
    parent: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    let Some(pl) = pl else { return };
    let pl_id = ctx.alloc();
    writer.push_optional_u32_attr(
        TagId::PrecursorList,
        pl_id,
        parent,
        ACC_ATTR_COUNT,
        Some(pl.precursors.len() as u32),
    );
    for p in &pl.precursors {
        flatten_precursor(writer, p, pl_id, ctx);
    }
}

fn flatten_product_list_opt(
    writer: &mut MetaParamWriter<'_>,
    pl: Option<&crate::mzml::structs::ProductList>,
    parent: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    let Some(pl) = pl else { return };
    let pl_id = ctx.alloc();
    writer.push_optional_u32_attr(
        TagId::ProductList,
        pl_id,
        parent,
        ACC_ATTR_COUNT,
        Some(pl.products.len() as u32),
    );
    for p in &pl.products {
        flatten_product(writer, p, pl_id, ctx);
    }
}

fn flatten_binary_data_array_list(
    writer: &mut MetaParamWriter<'_>,
    bda_list: Option<&BinaryDataArrayList>,
    parent_id: u32,
    ctx: &mut TraversalCtx<'_>,
    policy: ArrayPolicy,
) {
    let Some(list) = bda_list else { return };
    let list_id = ctx.alloc();
    writer.push_optional_u32_attr(
        TagId::BinaryDataArrayList,
        list_id,
        parent_id,
        ACC_ATTR_COUNT,
        Some(list.binary_data_arrays.len() as u32),
    );
    for bda in &list.binary_data_arrays {
        let bda_id = ctx.alloc();
        writer.touch(TagId::BinaryDataArray, bda_id, list_id);
        writer.push_schema_attrs(TagId::BinaryDataArray, bda_id, list_id, bda);
        emit_binary_data_array_cv_params(writer, bda_id, list_id, bda, policy);
    }
}

fn flatten_precursor(
    writer: &mut MetaParamWriter<'_>,
    precursor: &Precursor,
    parent_id: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    let p_id = ctx.alloc();
    writer.touch(TagId::Precursor, p_id, parent_id);
    writer.push_schema_attrs(TagId::Precursor, p_id, parent_id, precursor);
    if let Some(iw) = &precursor.isolation_window {
        let iw_id = ctx.alloc();
        writer.touch(TagId::IsolationWindow, iw_id, p_id);
        writer.push_cv_and_user_params(iw_id, p_id, &iw.cv_params, &iw.user_params);
    }
    if let Some(sil) = &precursor.selected_ion_list {
        let sil_id = ctx.alloc();
        writer.push_optional_u32_attr(
            TagId::SelectedIonList,
            sil_id,
            p_id,
            ACC_ATTR_COUNT,
            Some(sil.selected_ions.len() as u32),
        );
        for si in &sil.selected_ions {
            let si_id = ctx.alloc();
            writer.touch(TagId::SelectedIon, si_id, sil_id);
            writer.push_cv_and_user_params(si_id, sil_id, &si.cv_params, &si.user_params);
        }
    }
    if let Some(act) = &precursor.activation {
        let act_id = ctx.alloc();
        writer.touch(TagId::Activation, act_id, p_id);
        writer.push_cv_and_user_params(act_id, p_id, &act.cv_params, &act.user_params);
    }
}

fn flatten_product(
    writer: &mut MetaParamWriter<'_>,
    product: &Product,
    parent_id: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    let prod_id = ctx.alloc();
    writer.touch(TagId::Product, prod_id, parent_id);
    writer.push_schema_attrs(TagId::Product, prod_id, parent_id, product);
    writer.push_cv_and_user_params(prod_id, prod_id, &product.cv_params, &product.user_params);
    if let Some(iw) = &product.isolation_window {
        let iw_id = ctx.alloc();
        writer.touch(TagId::IsolationWindow, iw_id, prod_id);
        writer.push_cv_and_user_params(iw_id, prod_id, &iw.cv_params, &iw.user_params);
    }
}

fn flatten_scan_list(
    writer: &mut MetaParamWriter<'_>,
    scan_list: &ScanList,
    sl_id: u32,
    ctx: &mut TraversalCtx<'_>,
) {
    for scan in &scan_list.scans {
        let scan_id = ctx.alloc();
        writer.touch(TagId::Scan, scan_id, sl_id);
        writer.push_schema_attrs(TagId::Scan, scan_id, sl_id, scan);
        writer.push_cv_and_user_params(scan_id, sl_id, &scan.cv_params, &scan.user_params);
        if let Some(swl) = &scan.scan_window_list {
            let swl_id = ctx.alloc();
            writer.push_optional_u32_attr(
                TagId::ScanWindowList,
                swl_id,
                scan_id,
                ACC_ATTR_COUNT,
                Some(swl.scan_windows.len() as u32),
            );
            for sw in &swl.scan_windows {
                let sw_id = ctx.alloc();
                writer.touch(TagId::ScanWindow, sw_id, swl_id);
                writer.push_cv_and_user_params(sw_id, swl_id, &sw.cv_params, &sw.user_params);
            }
        }
    }
}

fn pack_global_meta(mzml: &MzML, ctx: &mut TraversalCtx<'_>) -> (PackedMeta, GlobalCounts) {
    let mut buffers: Vec<MetaParamBuffer> = Vec::new();

    let n_file_description = append_file_description_meta(mzml, ctx, &mut buffers);
    let n_run = append_run_meta(mzml, ctx, &mut buffers);
    let n_ref_param_groups = append_ref_param_groups_meta(mzml, ctx, &mut buffers);
    let n_samples = append_samples_meta(mzml, ctx, &mut buffers);
    let n_instrument_configs = append_instruments_meta(mzml, ctx, &mut buffers);
    let n_software = append_software_list_meta(mzml, ctx, &mut buffers);
    let n_data_processing = append_data_processing_list_meta(mzml, ctx, &mut buffers);
    let n_acquisition_settings = append_scan_settings_list_meta(mzml, ctx, &mut buffers);
    let n_cvs = append_cv_list_meta(mzml, ctx, &mut buffers);

    let counts = GlobalCounts {
        n_file_description,
        n_run,
        n_ref_param_groups,
        n_samples,
        n_instrument_configs,
        n_software,
        n_data_processing,
        n_acquisition_settings,
        n_cvs,
    };
    let mut builder = PackedMetaBuilder::new();
    for buffer in &buffers {
        builder.flush_buffer(buffer);
    }
    (builder.build(), counts)
}

fn append_file_description_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(fd) = &mzml.file_description else {
        return 0;
    };
    append_meta_buffer(buffers, |writer| {
        let fd_id = ctx.alloc();
        let fc_id = ctx.alloc();
        let sfl_id = ctx.alloc();

        writer.touch(TagId::FileDescription, fd_id, 0);
        writer.touch(TagId::FileContent, fc_id, fd_id);
        writer.push_ref_group_params(
            fc_id,
            fd_id,
            &fd.file_content.referenceable_param_group_refs,
            ctx.ref_groups,
        );
        writer.push_cv_and_user_params(
            fc_id,
            fd_id,
            &fd.file_content.cv_params,
            &fd.file_content.user_params,
        );

        writer.touch(TagId::SourceFileList, sfl_id, fd_id);
        writer.push_optional_u32_attr(
            TagId::SourceFileList,
            sfl_id,
            fd_id,
            ACC_ATTR_COUNT,
            Some(fd.source_file_list.source_file.len() as u32),
        );

        for sf in &fd.source_file_list.source_file {
            let sf_id = ctx.alloc();
            writer.touch(TagId::SourceFile, sf_id, sfl_id);
            writer.push_str_attr(TagId::SourceFile, sf_id, sfl_id, ACC_ATTR_ID, &sf.id);
            writer.push_str_attr(TagId::SourceFile, sf_id, sfl_id, ACC_ATTR_NAME, &sf.name);
            writer.push_str_attr(
                TagId::SourceFile,
                sf_id,
                sfl_id,
                ACC_ATTR_LOCATION,
                &sf.location,
            );
            writer.push_ref_group_params(
                sf_id,
                sfl_id,
                &sf.referenceable_param_group_ref,
                ctx.ref_groups,
            );
            writer.push_cv_and_user_params(sf_id, sfl_id, &sf.cv_param, &sf.user_param);
        }
        for contact in &fd.contacts {
            let c_id = ctx.alloc();
            writer.touch(TagId::Contact, c_id, fd_id);
            writer.push_ref_group_params(
                c_id,
                fd_id,
                &contact.referenceable_param_group_refs,
                ctx.ref_groups,
            );
            writer.push_cv_and_user_params(c_id, fd_id, &contact.cv_params, &contact.user_params);
        }
    });
    1
}

fn append_run_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let run = &mzml.run;
    append_meta_buffer(buffers, |writer| {
        let run_id = ctx.alloc();
        writer.touch(TagId::Run, run_id, 0);
        writer.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_ID, &run.id);
        if let Some(ts) = run.start_time_stamp.as_deref() {
            writer.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_START_TIME_STAMP, ts);
        }
        if let Some(r) = run.default_instrument_configuration_ref.as_deref() {
            writer.push_str_attr(
                TagId::Run,
                run_id,
                0,
                ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
                r,
            );
        }
        if let Some(r) = run.default_source_file_ref.as_deref() {
            writer.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_DEFAULT_SOURCE_FILE_REF, r);
        }
        if let Some(r) = run.sample_ref.as_deref() {
            writer.push_str_attr(TagId::Run, run_id, 0, ACC_ATTR_SAMPLE_REF, r);
        }
        if let Some(sfrl) = &run.source_file_ref_list {
            let sfrl_id = ctx.alloc();
            writer.touch(TagId::SourceFileRefList, sfrl_id, run_id);
            writer.push_optional_u32_attr(
                TagId::SourceFileRefList,
                sfrl_id,
                run_id,
                ACC_ATTR_COUNT,
                Some(sfrl.source_file_refs.len() as u32),
            );
            for sfr in &sfrl.source_file_refs {
                let sfr_id = ctx.alloc();
                writer.touch(TagId::SourceFileRef, sfr_id, sfrl_id);
                writer.push_str_attr(
                    TagId::SourceFileRef,
                    sfr_id,
                    sfrl_id,
                    ACC_ATTR_REF,
                    &sfr.r#ref,
                );
            }
        }
        for gr in &run.referenceable_param_group_refs {
            let gref_id = ctx.alloc();
            writer.touch(TagId::ReferenceableParamGroupRef, gref_id, run_id);
            writer.push_str_attr(
                TagId::ReferenceableParamGroupRef,
                gref_id,
                run_id,
                ACC_ATTR_REF,
                &gr.r#ref,
            );
        }
        writer.push_cv_and_user_params(run_id, 0, &run.cv_params, &run.user_params);
    });
    1
}

fn append_ref_param_groups_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(list) = &mzml.referenceable_param_group_list else {
        return 0;
    };
    for group in &list.referenceable_param_groups {
        append_meta_buffer(buffers, |writer| {
            let gid = ctx.alloc();
            writer.touch(TagId::ReferenceableParamGroup, gid, 0);
            writer.push_str_attr(
                TagId::ReferenceableParamGroup,
                gid,
                0,
                ACC_ATTR_ID,
                &group.id,
            );
            writer.push_cv_and_user_params(gid, 0, &group.cv_params, &group.user_params);
        });
    }
    list.referenceable_param_groups.len() as u32
}

fn append_samples_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(list) = &mzml.sample_list else {
        return 0;
    };
    for sample in &list.samples {
        append_meta_buffer(buffers, |writer| {
            let sid = ctx.alloc();
            writer.touch(TagId::Sample, sid, 0);
            writer.push_str_attr(TagId::Sample, sid, 0, ACC_ATTR_ID, &sample.id);
            writer.push_str_attr(TagId::Sample, sid, 0, ACC_ATTR_NAME, &sample.name);
            if let Some(gr) = &sample.referenceable_param_group_ref {
                writer.push_ref_group_params(sid, 0, slice::from_ref(gr), ctx.ref_groups);
            }
        });
    }
    list.samples.len() as u32
}

fn emit_instrument_component(
    writer: &mut MetaParamWriter<'_>,
    tag: TagId,
    order: Option<u32>,
    parent_id: u32,
    group_refs: &[ReferenceableParamGroupRef],
    cv_params: &[CvParam],
    user_params: &[UserParam],
    ctx: &mut TraversalCtx<'_>,
) {
    let comp_id = ctx.alloc();
    writer.touch(tag, comp_id, parent_id);
    writer.push_optional_u32_attr(tag, comp_id, parent_id, ACC_ATTR_ORDER, order);
    writer.push_ref_group_params(comp_id, parent_id, group_refs, ctx.ref_groups);
    writer.push_cv_and_user_params(comp_id, parent_id, cv_params, user_params);
}

fn append_instruments_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(list) = &mzml.instrument_list else {
        return 0;
    };
    for inst in &list.instrument {
        append_meta_buffer(buffers, |writer| {
            let iid = ctx.alloc();
            writer.touch(TagId::Instrument, iid, 0);
            writer.push_str_attr(TagId::Instrument, iid, 0, ACC_ATTR_ID, &inst.id);
            writer.push_ref_group_params(
                iid,
                0,
                &inst.referenceable_param_group_ref,
                ctx.ref_groups,
            );
            writer.push_cv_and_user_params(iid, 0, &inst.cv_param, &inst.user_param);
            if let Some(cl) = &inst.component_list {
                for s in &cl.source {
                    emit_instrument_component(
                        writer,
                        TagId::ComponentSource,
                        s.order,
                        iid,
                        &s.referenceable_param_group_ref,
                        &s.cv_param,
                        &s.user_param,
                        ctx,
                    );
                }
                for a in &cl.analyzer {
                    emit_instrument_component(
                        writer,
                        TagId::ComponentAnalyzer,
                        a.order,
                        iid,
                        &a.referenceable_param_group_ref,
                        &a.cv_param,
                        &a.user_param,
                        ctx,
                    );
                }
                for d in &cl.detector {
                    emit_instrument_component(
                        writer,
                        TagId::ComponentDetector,
                        d.order,
                        iid,
                        &d.referenceable_param_group_ref,
                        &d.cv_param,
                        &d.user_param,
                        ctx,
                    );
                }
            }
        });
    }
    list.instrument.len() as u32
}

fn append_software_list_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(list) = &mzml.software_list else {
        return 0;
    };
    for sw in &list.software {
        append_meta_buffer(buffers, |writer| {
            let swid = ctx.alloc();
            writer.touch(TagId::Software, swid, 0);
            writer.push_str_attr(TagId::Software, swid, 0, ACC_ATTR_ID, &sw.id);
            let version = sw
                .version
                .as_deref()
                .or_else(|| sw.software_param.first().and_then(|p| p.version.as_deref()));
            if let Some(v) = version {
                writer.push_str_attr(TagId::Software, swid, 0, ACC_ATTR_VERSION, v);
            }
            for sp in &sw.software_param {
                let pid = ctx.alloc();
                writer.touch(TagId::SoftwareParam, pid, swid);
                writer.push_one(
                    TagId::SoftwareParam,
                    pid,
                    swid,
                    CvParam {
                        cv_ref: sp.cv_ref.clone(),
                        accession: Some(sp.accession.clone()),
                        name: sp.name.clone(),
                        value: Some(String::new()),
                        unit_cv_ref: None,
                        unit_name: None,
                        unit_accession: None,
                    },
                );
            }
            writer.push_cv_and_user_params(swid, 0, &sw.cv_param, &sw.user_params);
        });
    }
    list.software.len() as u32
}

fn append_data_processing_list_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(list) = &mzml.data_processing_list else {
        return 0;
    };
    for dp in &list.data_processing {
        append_meta_buffer(buffers, |writer| {
            let dp_id = ctx.alloc();
            writer.touch(TagId::DataProcessing, dp_id, 0);
            writer.push_str_attr(TagId::DataProcessing, dp_id, 0, ACC_ATTR_ID, &dp.id);
            for pm in &dp.processing_method {
                let pm_id = ctx.alloc();
                writer.touch(TagId::ProcessingMethod, pm_id, dp_id);
                writer.push_ref_group_params(
                    pm_id,
                    dp_id,
                    &pm.referenceable_param_group_ref,
                    ctx.ref_groups,
                );
                writer.push_cv_and_user_params(pm_id, dp_id, &pm.cv_param, &pm.user_param);
            }
        });
    }
    list.data_processing.len() as u32
}

fn append_scan_settings_list_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(list) = &mzml.scan_settings_list else {
        return 0;
    };
    for ss in &list.scan_settings {
        append_meta_buffer(buffers, |writer| {
            let ss_id = ctx.alloc();
            writer.touch(TagId::ScanSettings, ss_id, 0);
            if let Some(id) = ss.id.as_deref() {
                writer.push_str_attr(TagId::ScanSettings, ss_id, 0, ACC_ATTR_ID, id);
            }
            if let Some(r) = ss.instrument_configuration_ref.as_deref() {
                writer.push_str_attr(
                    TagId::ScanSettings,
                    ss_id,
                    0,
                    ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
                    r,
                );
            }
            if let Some(sfrl) = &ss.source_file_ref_list {
                let sfrl_id = ctx.alloc();
                writer.touch(TagId::SourceFileRefList, sfrl_id, ss_id);
                writer.push_optional_u32_attr(
                    TagId::SourceFileRefList,
                    sfrl_id,
                    ss_id,
                    ACC_ATTR_COUNT,
                    Some(sfrl.source_file_refs.len() as u32),
                );
                for sfr in &sfrl.source_file_refs {
                    let sfr_id = ctx.alloc();
                    writer.touch(TagId::SourceFileRef, sfr_id, sfrl_id);
                    writer.push_str_attr(
                        TagId::SourceFileRef,
                        sfr_id,
                        sfrl_id,
                        ACC_ATTR_REF,
                        &sfr.r#ref,
                    );
                }
            }
            writer.push_ref_group_params(
                ss_id,
                0,
                &ss.referenceable_param_group_refs,
                ctx.ref_groups,
            );
            writer.push_cv_and_user_params(ss_id, 0, &ss.cv_params, &ss.user_params);
            if let Some(tl) = &ss.target_list {
                for target in &tl.targets {
                    let t_id = ctx.alloc();
                    writer.touch(TagId::Target, t_id, ss_id);
                    writer.push_ref_group_params(
                        t_id,
                        ss_id,
                        &target.referenceable_param_group_refs,
                        ctx.ref_groups,
                    );
                    writer.push_cv_and_user_params(
                        t_id,
                        ss_id,
                        &target.cv_params,
                        &target.user_params,
                    );
                }
            }
        });
    }
    list.scan_settings.len() as u32
}

fn append_cv_list_meta(
    mzml: &MzML,
    ctx: &mut TraversalCtx<'_>,
    buffers: &mut Vec<MetaParamBuffer>,
) -> u32 {
    let Some(cv_list) = &mzml.cv_list else {
        return 0;
    };
    if cv_list.cv.is_empty() {
        return 0;
    }
    let cv_list_id = ctx.alloc();
    let cv_count = cv_list.cv.len() as u32;
    for (i, cv) in cv_list.cv.iter().enumerate() {
        append_meta_buffer(buffers, |writer| {
            if i == 0 {
                writer.touch(TagId::CvList, cv_list_id, 0);
                writer.push_optional_u32_attr(
                    TagId::CvList,
                    cv_list_id,
                    0,
                    ACC_ATTR_COUNT,
                    Some(cv_count),
                );
            }
            let cv_id = ctx.alloc();
            writer.touch(TagId::Cv, cv_id, cv_list_id);
            writer.push_str_attr(TagId::Cv, cv_id, cv_list_id, ACC_ATTR_LABEL, &cv.id);
            if let Some(n) = cv.full_name.as_deref().filter(|s| !s.is_empty()) {
                writer.push_str_attr(TagId::Cv, cv_id, cv_list_id, ACC_ATTR_CV_FULL_NAME, n);
            }
            if let Some(v) = cv.version.as_deref().filter(|s| !s.is_empty()) {
                writer.push_str_attr(TagId::Cv, cv_id, cv_list_id, ACC_ATTR_CV_VERSION, v);
            }
            if let Some(u) = cv.uri.as_deref().filter(|s| !s.is_empty()) {
                writer.push_str_attr(TagId::Cv, cv_id, cv_list_id, ACC_ATTR_CV_URI, u);
            }
        });
    }
    cv_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_pool_empty_value_gives_kind_2() {
        let mut pool = ValuePool::new();
        assert_eq!(pool.encode(None), ValueEncoding { kind: 2, index: 0 });
        assert_eq!(pool.encode(Some("")), ValueEncoding { kind: 2, index: 0 });
    }

    #[test]
    fn value_pool_numeric_increments_index() {
        let mut pool = ValuePool::new();
        assert_eq!(
            pool.encode(Some("1.5")),
            ValueEncoding { kind: 0, index: 0 }
        );
        assert_eq!(
            pool.encode(Some("2.5")),
            ValueEncoding { kind: 0, index: 1 }
        );
        assert_eq!(pool.numeric_values, vec![1.5f64, 2.5f64]);
    }

    #[test]
    fn value_pool_string_increments_index() {
        let mut pool = ValuePool::new();
        assert_eq!(
            pool.encode(Some("hello")),
            ValueEncoding { kind: 1, index: 0 }
        );
        assert_eq!(
            pool.encode(Some("world")),
            ValueEncoding { kind: 1, index: 1 }
        );
        assert_eq!(&pool.string_bytes[..5], b"hello");
    }

    #[test]
    fn value_pool_string_offsets_are_cumulative() {
        let mut pool = ValuePool::new();
        pool.encode(Some("ab"));
        pool.encode(Some("cde"));
        assert_eq!(pool.string_offsets, vec![0, 2]);
        assert_eq!(pool.string_lengths, vec![2, 3]);
    }

    #[test]
    fn meta_param_buffer_push_records_owner_id() {
        let mut buffer = MetaParamBuffer::new();
        buffer.push(TagId::CvParam, 1, 0, empty_cv_param());
        assert_eq!(buffer.rows.len(), 1);
        assert_eq!(buffer.rows[0].owner_id, 1);
    }

    #[test]
    fn meta_param_buffer_clear_empties_rows() {
        let mut buffer = MetaParamBuffer::new();
        buffer.push(TagId::CvParam, 1, 0, empty_cv_param());
        buffer.clear();
        assert!(buffer.rows.is_empty());
    }

    #[test]
    fn meta_param_buffer_normalize_moves_name_to_value_for_attr_cv() {
        let mut buffer = MetaParamBuffer::new();
        buffer.rows.push(MetaParamRow {
            cv_param: CvParam {
                cv_ref: Some(CV_REF_ATTR.to_string()),
                accession: Some(format!("{}:9910001", CV_REF_ATTR)),
                name: "my-id".to_string(),
                value: None,
                unit_cv_ref: None,
                unit_name: None,
                unit_accession: None,
            },
            tag_id: TagId::CvParam as u8,
            owner_id: 1,
            parent_id: 0,
        });
        buffer.normalize_attr_cv_values();
        assert_eq!(buffer.rows[0].cv_param.value.as_deref(), Some("my-id"));
        assert!(buffer.rows[0].cv_param.name.is_empty());
    }

    #[test]
    fn meta_param_buffer_normalize_skips_non_attr_cv() {
        let mut buffer = MetaParamBuffer::new();
        buffer.rows.push(MetaParamRow {
            cv_param: CvParam {
                cv_ref: Some("MS".to_string()),
                accession: Some("MS:1000514".to_string()),
                name: "m/z array".to_string(),
                value: None,
                unit_cv_ref: None,
                unit_name: None,
                unit_accession: None,
            },
            tag_id: TagId::CvParam as u8,
            owner_id: 1,
            parent_id: 0,
        });
        buffer.normalize_attr_cv_values();
        assert_eq!(buffer.rows[0].cv_param.name, "m/z array");
        assert!(buffer.rows[0].cv_param.value.is_none());
    }

    #[test]
    fn packed_meta_builder_empty_produces_single_sentinel() {
        let meta = PackedMetaBuilder::new().build();
        assert_eq!(meta.index_offsets, vec![0]);
        assert!(meta.ids.is_empty());
    }

    #[test]
    fn packed_meta_builder_flush_buffer_advances_index_offsets() {
        let mut builder = PackedMetaBuilder::new();
        let mut buffer = MetaParamBuffer::new();
        buffer.push(TagId::CvParam, 1, 0, empty_cv_param());
        buffer.push(TagId::CvParam, 1, 0, empty_cv_param());
        builder.flush_buffer(&buffer);
        let meta = builder.build();
        assert_eq!(meta.index_offsets, vec![0, 2]);
        assert_eq!(meta.ids.len(), 2);
    }

    #[test]
    fn encode_user_param_with_value_uses_separator() {
        let p = UserParam {
            name: "my-param".to_string(),
            value: Some("42".to_string()),
            r#type: None,
            unit_cv_ref: None,
            unit_name: None,
            unit_accession: None,
        };
        let encoded = encode_user_param_as_cv(&p).value.unwrap();
        let parts: Vec<&str> = encoded.splitn(2, USER_PARAM_NAME_VALUE_SEPARATOR).collect();
        assert_eq!(parts[0], "my-param");
        assert_eq!(parts[1], "42");
    }

    #[test]
    fn compress_bytes_if_enabled_level_zero_is_identity() {
        let input = vec![1u8, 2, 3, 4];
        assert_eq!(compress_bytes_if_enabled(input.clone(), 0), input);
    }

    #[test]
    fn array_type_accession_from_bda_returns_mz_array() {
        let bda = BinaryDataArray {
            cv_params: vec![CvParam {
                cv_ref: Some("MS".to_string()),
                accession: Some("MS:1000514".to_string()),
                name: "m/z array".to_string(),
                value: None,
                unit_cv_ref: None,
                unit_name: None,
                unit_accession: None,
            }],
            ..Default::default()
        };
        assert_eq!(
            array_type_accession_from_binary_data_array(&bda),
            ACCESSION_MZ_ARRAY
        );
    }

    #[test]
    fn array_type_accession_from_bda_returns_zero_when_absent() {
        assert_eq!(
            array_type_accession_from_binary_data_array(&BinaryDataArray::default()),
            0
        );
    }

    #[test]
    fn collector_alloc_starts_at_one_and_increments() {
        let ref_groups = HashMap::new();
        let mut collector = MetaCollector::new(&ref_groups);
        assert_eq!(collector.alloc(), 1);
        assert_eq!(collector.alloc(), 2);
        assert_eq!(collector.alloc(), 3);
    }

    #[test]
    fn collector_global_meta_on_empty_mzml_produces_run_buffer() {
        let mzml = MzML::default();
        let ref_groups = build_ref_group_lookup(&mzml);
        let mut collector = MetaCollector::new(&ref_groups);
        let (meta, counts) = collector.collect_global_meta(&mzml);
        assert_eq!(counts.n_run, 1);
        assert!(!meta.ids.is_empty());
    }

    #[test]
    fn collector_spectrum_meta_on_empty_list_produces_empty_packed_meta() {
        let ref_groups = HashMap::new();
        let mut collector = MetaCollector::new(&ref_groups);
        let spectra: &[Spectrum] = &[];
        let policy = ArrayPolicy {
            x_array_accession: ACCESSION_MZ_ARRAY,
            y_array_accession: ACCESSION_INTENSITY_ARRAY,
            force_f32: false,
        };
        let meta = collector.collect_item_list_meta::<Spectrum, MzML>(spectra, 0, None, policy);
        assert_eq!(meta.index_offsets, vec![0]);
        assert!(meta.ids.is_empty());
    }

    #[test]
    fn serialize_packed_meta_roundtrip_size() {
        let meta = PackedMetaBuilder::new().build();
        let bytes = serialize_packed_meta(&meta);
        assert_eq!(bytes.len(), packed_meta_byte_size(&meta));
    }

    #[test]
    fn compressed_meta_sections_level_zero_preserves_bytes() {
        let meta = PackedMetaBuilder::new().build();
        let counts = GlobalCounts {
            n_file_description: 0,
            n_run: 1,
            n_ref_param_groups: 0,
            n_samples: 0,
            n_instrument_configs: 0,
            n_software: 0,
            n_data_processing: 0,
            n_acquisition_settings: 0,
            n_cvs: 0,
        };
        let compressed = CompressedMetaSections::build(&meta, &meta, &meta, &counts, 0);
        let raw_s = serialize_packed_meta(&meta);
        assert_eq!(compressed.spectrum_bytes, raw_s);
        assert_eq!(compressed.spectrum_uncompressed_size, raw_s.len() as u64);
    }

    #[test]
    fn array_policy_identifies_xy_arrays() {
        let policy = ArrayPolicy {
            x_array_accession: ACCESSION_MZ_ARRAY,
            y_array_accession: ACCESSION_INTENSITY_ARRAY,
            force_f32: true,
        };
        assert!(policy.is_xy_array(ACCESSION_MZ_ARRAY));
        assert!(policy.is_xy_array(ACCESSION_INTENSITY_ARRAY));
        assert!(!policy.is_xy_array(ACCESSION_TIME_ARRAY));
        assert!(policy.should_force_f32(ACCESSION_MZ_ARRAY));
    }

    #[test]
    fn array_policy_no_force_when_disabled() {
        let policy = ArrayPolicy {
            x_array_accession: ACCESSION_MZ_ARRAY,
            y_array_accession: ACCESSION_INTENSITY_ARRAY,
            force_f32: false,
        };
        assert!(!policy.should_force_f32(ACCESSION_MZ_ARRAY));
    }

    #[test]
    fn build_ref_group_lookup_empty_mzml() {
        let mzml = MzML::default();
        let lookup = build_ref_group_lookup(&mzml);
        assert!(lookup.is_empty());
    }
}
