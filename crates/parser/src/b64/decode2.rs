use std::{
    collections::{HashMap, HashSet},
    io::Read,
};

use crate::mzml::{
    attr_meta::format_accession,
    cv_table,
    schema::{SchemaNode, SchemaTree as Schema, TagId, schema},
    structs::*,
};

const HDR_CODEC_MASK: u8 = 0x0F;
const HDR_CODEC_ZSTD: u8 = 1;

pub fn decode2(bytes: &[u8]) -> Result<MzML, String> {
    let schema = schema();
    let header = parse_header(bytes)?;
    Ok(MzML {
        cv_list: parse_cv_list(schema, bytes),
        file_description: parse_file_description(schema, bytes),
        referenceable_param_group_list: parse_referenceable_param_group_list(schema, bytes),
        sample_list: parse_sample_list(schema, bytes),
        instrument_list: parse_instrument_list(schema, bytes),
        software_list: parse_software_list(schema, bytes),
        data_processing_list: parse_data_processing_list(schema, bytes),
        scan_settings_list: parse_scan_settings_list(schema, bytes),
        run: parse_run(schema, bytes, &header),
    })
}

#[inline]
fn child_node<'a>(parent: Option<&'a SchemaNode>, tag: TagId) -> Option<&'a SchemaNode> {
    let p = parent?;
    let key = p.child_key_for_tag(tag)?;
    p.children.get(key)
}

/// <run>
#[inline]
fn parse_run(schema: &Schema, bytes: &[u8], header: &Header) -> Run {
    Run {
        spectrum_list: parse_spectrum_list(schema, bytes, header),
        chromatogram_list: parse_chromatogram_list(schema, bytes),
        ..Default::default()
    }
}

/// <spectrumList>
#[inline]
fn parse_spectrum_list(schema: &Schema, bytes: &[u8], header: &Header) -> Option<SpectrumList> {
    let root_node = schema.root_by_tag(TagId::SpectrumList)?;
    let spectrum_node = child_node(Some(root_node), TagId::Spectrum);
    let _ = (root_node, spectrum_node, bytes);

    Some(SpectrumList {
        count: Some(0),
        default_data_processing_ref: None,
        spectra: Vec::new(),
    })
}

/// <chromatogramList>
#[inline]
fn parse_chromatogram_list(schema: &Schema, bytes: &[u8]) -> Option<ChromatogramList> {
    let root_node = schema.root_by_tag(TagId::ChromatogramList)?;
    let chromatogram_node = child_node(Some(root_node), TagId::Chromatogram);
    let _ = (root_node, chromatogram_node, bytes);

    Some(ChromatogramList {
        count: Some(0),
        default_data_processing_ref: None,
        chromatograms: Vec::new(),
    })
}

/// <spectrum>
#[inline]
fn decode_spectrum(schema: &Schema, bytes: &[u8], metadata: &Vec<Metadatum>) -> Spectrum {
    let spectrum_node = schema.root_by_tag(TagId::Spectrum);
    let _ = (spectrum_node, bytes);

    Spectrum {
        id: String::new(),
        index: None,
        scan_number: None,
        default_array_length: None,
        native_id: None,
        data_processing_ref: None,
        source_file_ref: None,
        spot_id: None,
        ms_level: None,
        referenceable_param_group_refs: Vec::new(),
        cv_params: Vec::new(),
        user_params: Vec::new(),
        spectrum_description: None,
        scan_list: None,
        precursor_list: None,
        product_list: None,
        binary_data_array_list: None,
    }
}

/// <chromatogram>
#[inline]
fn decode_chromatogram(schema: &Schema, bytes: &[u8], header: &Header) -> Chromatogram {
    let chromatogram_node = schema.root_by_tag(TagId::Chromatogram);
    let _ = (chromatogram_node, bytes);

    Chromatogram {
        id: String::new(),
        native_id: None,
        index: None,
        default_array_length: None,
        data_processing_ref: None,
        referenceable_param_group_refs: Vec::new(),
        cv_params: Vec::new(),
        user_params: Vec::new(),
        precursor: None,
        product: None,
        binary_data_array_list: None,
    }
}

#[inline]
fn split_prefix(acc: &str) -> Option<(&str, &str)> {
    acc.split_once(':')
}

#[inline]
fn opt_unit_cv_ref(unit_acc: &Option<String>) -> Option<String> {
    unit_acc
        .as_deref()
        .and_then(|u| split_prefix(u))
        .map(|(p, _)| p.to_string())
}

#[inline]
fn value_to_opt_string(v: &MetadatumValue) -> Option<String> {
    match v {
        MetadatumValue::Empty => None,
        MetadatumValue::Text(s) => Some(s.clone()),
        MetadatumValue::Number(x) => Some(x.to_string()),
    }
}

#[inline]
fn is_cv_prefix(p: &str) -> bool {
    matches!(p, "MS" | "UO" | "NCIT" | "PEFF")
}

#[inline]
fn allowed_bda_cv_accessions(schema: &Schema) -> Option<HashSet<&str>> {
    let list = schema.root_by_tag(TagId::BinaryDataArrayList)?;
    let bda = child_node(Some(list), TagId::BinaryDataArray)?;
    let cvp = child_node(Some(bda), TagId::CvParam)?;

    Some(cvp.accessions.iter().map(|s| s.as_str()).collect())
}

/// <binaryDataArrayList>
#[inline]
pub fn parse_binary_data_array_list(
    schema: &Schema,
    // bytes: &[u8],
    metadata: &Vec<Metadatum>,
) -> Option<BinaryDataArrayList> {
    // let _ = bytes;

    let list_node = find_node_by_tag(schema, TagId::BinaryDataArrayList)?;
    let bda_node = child_node(Some(list_node), TagId::BinaryDataArray)?; // list -> bda
    let mut groups: std::collections::HashMap<u32, Vec<&Metadatum>> =
        std::collections::HashMap::new();
    for m in metadata {
        if matches!(
            m.tag_id,
            TagId::BinaryDataArray | TagId::CvParam | TagId::UserParam
        ) {
            groups.entry(m.item_index).or_default().push(m);
        }
    }

    let mut keys: Vec<u32> = groups.keys().copied().collect();
    keys.sort_unstable();

    let mut binary_data_arrays = Vec::with_capacity(keys.len());
    for k in keys {
        let group = &groups[&k];
        binary_data_arrays.push(parse_binary_data_array(schema, bda_node, group));
    }

    Some(BinaryDataArrayList {
        count: Some(binary_data_arrays.len()),
        binary_data_arrays,
    })
}

#[inline]
fn parse_binary_data_array(
    _schema: &Schema,
    bda_node: &SchemaNode,
    metadata: &[&Metadatum],
) -> BinaryDataArray {
    // debug_assert!(child_node(Some(bda_node), TagId::Binary).is_some());
    // allowed accessions belong here too (cvParam child of binaryDataArray)
    let allowed: std::collections::HashSet<&str> = child_node(Some(bda_node), TagId::CvParam)
        .map(|n| n.accessions.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();

    let mut out = BinaryDataArray {
        array_length: None,
        encoded_length: None,
        data_processing_ref: None,
        referenceable_param_group_refs: Vec::new(),
        cv_params: Vec::new(),
        user_params: Vec::new(),
        is_f32: None,
        is_f64: None,
        decoded_binary_f32: Vec::new(),
        decoded_binary_f64: Vec::new(),
    };

    for m in metadata {
        let Some(acc) = m.accession.as_deref() else {
            continue;
        };
        let Some((prefix, _)) = acc.split_once(':') else {
            continue;
        };

        if prefix == "B000" {
            continue; // attributes; later map to fields
        }

        let value = value_to_opt_string(&m.value);
        let unit_cv_ref = unit_cv_ref(&m.unit_accession);

        if is_cv_prefix(prefix) {
            if !allowed.is_empty() && !allowed.contains(acc) {
                // policy: keep unknowns as userParam (or drop)
                out.user_params.push(UserParam {
                    name: acc.to_string(),
                    r#type: None,
                    unit_accession: m.unit_accession.clone(),
                    unit_cv_ref,
                    unit_name: None,
                    value,
                });
                continue;
            }

            out.cv_params.push(CvParam {
                cv_ref: Some(prefix.to_string()),
                accession: Some(acc.to_string()),
                name: cv_table::get(acc)
                    .and_then(|v| v.as_str())
                    .unwrap_or(acc)
                    .to_string(),
                value,
                unit_cv_ref,
                unit_name: None,
                unit_accession: m.unit_accession.clone(),
            });
        } else {
            out.user_params.push(UserParam {
                name: cv_table::get(acc)
                    .and_then(|v| v.as_str())
                    .unwrap_or(acc)
                    .to_string(),
                r#type: None,
                unit_accession: m.unit_accession.clone(),
                unit_cv_ref,
                unit_name: None,
                value,
            });
        }
    }

    let has_f32 = out
        .cv_params
        .iter()
        .any(|p| p.accession.as_deref() == Some("MS:1000521"));
    let has_f64 = out
        .cv_params
        .iter()
        .any(|p| p.accession.as_deref() == Some("MS:1000523"));
    if has_f32 {
        out.is_f32 = Some(true);
    }
    if has_f64 {
        out.is_f64 = Some(true);
    }

    out
}

#[inline]
fn find_node_by_tag<'a>(schema: &'a Schema, tag: TagId) -> Option<&'a SchemaNode> {
    if let Some(n) = schema.root_by_tag(tag) {
        return Some(n);
    }

    for root in schema.roots.values() {
        if let Some(n) = find_node_by_tag_rec(root, tag) {
            return Some(n);
        }
    }
    None
}

fn find_node_by_tag_rec<'a>(node: &'a SchemaNode, tag: TagId) -> Option<&'a SchemaNode> {
    if node.self_tags.iter().any(|&t| t == tag) {
        return Some(node);
    }
    for child in node.children.values() {
        if let Some(n) = find_node_by_tag_rec(child, tag) {
            return Some(n);
        }
    }
    None
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
    pub tag_id: TagId,
    pub accession: Option<String>,
    pub unit_accession: Option<String>,
    pub value: MetadatumValue,
}

#[inline]
fn take<'a>(
    bytes: &'a [u8],
    pos: &mut usize,
    n: usize,
    field: &'static str,
) -> Result<&'a [u8], String> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| format!("overflow while reading {field}"))?;
    if end > bytes.len() {
        return Err(format!(
            "unexpected EOF while reading {field}: need {n} bytes at pos {pos}, len {}",
            bytes.len()
        ));
    }
    let out = &bytes[*pos..end];
    *pos = end;
    Ok(out)
}

#[inline]
fn read_u32_vec(bytes: &[u8], pos: &mut usize, n: usize) -> Result<Vec<u32>, String> {
    let raw = take(bytes, pos, n * 4, "u32 vector")?;
    let mut out = Vec::with_capacity(n);
    for chunk in raw.chunks_exact(4) {
        out.push(u32::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(out)
}

#[inline]
fn read_f64_vec(bytes: &[u8], pos: &mut usize, n: usize) -> Result<Vec<f64>, String> {
    let raw = take(bytes, pos, n * 8, "f64 vector")?;
    let mut out = Vec::with_capacity(n);
    for chunk in raw.chunks_exact(8) {
        out.push(f64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(out)
}

#[inline]
fn vs_len_bytes(vk: &[u8], vi: &[u32], voff: &[u32], vlen: &[u32]) -> Result<usize, String> {
    let mut max_end = 0usize;

    for j in 0..vk.len() {
        if vk[j] != 1 {
            continue;
        }
        let idx = vi[j] as usize;
        if idx >= voff.len() || idx >= vlen.len() {
            return Err("string VI out of range".to_string());
        }
        let end = (voff[idx] as usize)
            .checked_add(vlen[idx] as usize)
            .ok_or_else(|| "VOFF+VLEN overflow".to_string())?;
        if end > max_end {
            max_end = end;
        }
    }

    Ok(max_end)
}

pub fn parse_metadata(
    bytes: &[u8],
    item_count: u32,
    meta_count: u32,
    num_count: u32,
    str_count: u32,
    compressed: bool,
    reserved_flags: u8,
) -> Result<Vec<Metadatum>, String> {
    let codec_id = reserved_flags & HDR_CODEC_MASK;

    let owned;
    let bytes = if compressed {
        if codec_id != HDR_CODEC_ZSTD {
            return Err(format!("unsupported metadata codec_id={codec_id}"));
        }
        owned = decompress_zstd_allow_aligned_padding(bytes)?;
        owned.as_slice()
    } else {
        bytes
    };

    let item_count = item_count as usize;
    let meta_count = meta_count as usize;
    let num_count = num_count as usize;
    let str_count = str_count as usize;

    let mut pos = 0;

    let ci = read_u32_vec(bytes, &mut pos, item_count + 1)?;

    let mti = take(bytes, &mut pos, meta_count, "metadatum tag id")?;
    let mri = take(bytes, &mut pos, meta_count, "metadatum ref id")?;
    let man = read_u32_vec(bytes, &mut pos, meta_count)?;
    let muri = take(bytes, &mut pos, meta_count, "metadatum unit ref id")?;
    let muan = read_u32_vec(bytes, &mut pos, meta_count)?;
    let vk = take(bytes, &mut pos, meta_count, "metadatum value kind")?;
    let vi = read_u32_vec(bytes, &mut pos, meta_count)?;

    let vn = read_f64_vec(bytes, &mut pos, num_count)?;
    let voff = read_u32_vec(bytes, &mut pos, str_count)?;
    let vlen = read_u32_vec(bytes, &mut pos, str_count)?;

    let vs_needed = vs_len_bytes(vk, &vi, &voff, &vlen)?;
    let vs = take(bytes, &mut pos, vs_needed, "string values")?;

    if !compressed {
        let trailing = &bytes[pos..];
        if trailing.len() > 7 || trailing.iter().any(|&b| b != 0) {
            return Err("trailing bytes in metadata section".to_string());
        }
    } else if pos != bytes.len() {
        return Err("trailing bytes in decompressed metadata section".to_string());
    }

    if ci.is_empty() || ci[0] != 0 {
        return Err("CI[0] must be 0".to_string());
    }
    if ci[item_count] as usize != meta_count {
        return Err("CI[last] must equal meta_count".to_string());
    }

    let mut prev = 0u32;
    for &x in &ci {
        if x < prev || (x as usize) > meta_count {
            return Err("CI is not monotonic or out of range".to_string());
        }
        prev = x;
    }

    let mut out = Vec::with_capacity(meta_count);

    for item_index in 0..item_count {
        let start = ci[item_index] as usize;
        let end = ci[item_index + 1] as usize;

        for j in start..end {
            let tag_id = TagId::from_u8(mti[j]).unwrap_or(TagId::Unknown);

            let value = match vk[j] {
                0 => {
                    let idx = vi[j] as usize;
                    if idx >= vn.len() {
                        return Err("numeric VI out of range".to_string());
                    }
                    MetadatumValue::Number(vn[idx])
                }
                1 => {
                    let idx = vi[j] as usize;
                    if idx >= voff.len() || idx >= vlen.len() {
                        return Err("string VI out of range".to_string());
                    }
                    let off = voff[idx] as usize;
                    let len = vlen[idx] as usize;
                    if off.checked_add(len).map_or(true, |e| e > vs.len()) {
                        return Err("string slice out of bounds".to_string());
                    }
                    MetadatumValue::Text(String::from_utf8_lossy(&vs[off..off + len]).into_owned())
                }
                2 => MetadatumValue::Empty,
                _ => MetadatumValue::Empty,
            };

            let accession = format_accession(mri[j], man[j]);
            let unit_accession = format_accession(muri[j], muan[j]);
            out.push(Metadatum {
                item_index: item_index as u32,
                tag_id,
                accession,
                unit_accession,
                value,
            });
        }
    }

    Ok(out)
}

/// <cvList>
#[inline]
fn parse_cv_list(_schema: &Schema, _bytes: &[u8]) -> Option<CvList> {
    None
}

/// <fileDescription>
#[inline]
fn parse_file_description(_schema: &Schema, _bytes: &[u8]) -> FileDescription {
    FileDescription::default()
}

/// <referenceableParamGroupList>
#[inline]
fn parse_referenceable_param_group_list(
    _schema: &Schema,
    _bytes: &[u8],
) -> Option<ReferenceableParamGroupList> {
    None
}

/// <sampleList>
#[inline]
fn parse_sample_list(_schema: &Schema, _bytes: &[u8]) -> Option<SampleList> {
    None
}

/// <instrumentList>
#[inline]
fn parse_instrument_list(_schema: &Schema, _bytes: &[u8]) -> Option<InstrumentList> {
    None
}

/// <softwareList>
#[inline]
fn parse_software_list(_schema: &Schema, _bytes: &[u8]) -> Option<SoftwareList> {
    None
}

/// <dataProcessingList>
#[inline]
fn parse_data_processing_list(_schema: &Schema, _bytes: &[u8]) -> Option<DataProcessingList> {
    None
}

/// <scanSettingsList>
#[inline]
fn parse_scan_settings_list(_schema: &Schema, _bytes: &[u8]) -> Option<ScanSettingsList> {
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub file_signature: [u8; 4],
    pub endianness_flag: u8,
    pub reserved_alignment: [u8; 3],

    pub off_spec_index: u64,
    pub off_chrom_index: u64,
    pub off_spec_meta: u64,
    pub off_chrom_meta: u64,
    pub off_global_meta: u64,

    pub size_container_spect_x: u64,
    pub off_container_spect_x: u64,
    pub size_container_spect_y: u64,
    pub off_container_spect_y: u64,
    pub size_container_chrom_x: u64,
    pub off_container_chrom_x: u64,
    pub size_container_chrom_y: u64,
    pub off_container_chrom_y: u64,

    pub spectrum_count: u32,
    pub chrom_count: u32,

    pub spec_meta_count: u32,
    pub spec_num_count: u32,
    pub spec_str_count: u32,

    pub chrom_meta_count: u32,
    pub chrom_num_count: u32,
    pub chrom_str_count: u32,

    pub global_meta_count: u32,
    pub global_num_count: u32,
    pub global_str_count: u32,

    pub block_count_spect_x: u32,
    pub block_count_spect_y: u32,
    pub block_count_chrom_x: u32,
    pub block_count_chrom_y: u32,

    pub reserved_flags: u8,
    pub chrom_x_format: u8,
    pub chrom_y_format: u8,
    pub spect_x_format: u8,
    pub spect_y_format: u8,
    pub compression_level: u8,
    pub array_filter: u8,

    pub reserved: [u8; 13],
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    #[inline]
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    #[inline]
    fn need(&self, n: usize, field: &str) -> Result<(), String> {
        if self.pos + n <= self.bytes.len() {
            Ok(())
        } else {
            Err(format!(
                "header: not enough bytes for {field} at offset {} (need {n}, have {})",
                self.pos,
                self.bytes.len().saturating_sub(self.pos)
            ))
        }
    }

    #[inline]
    fn read_u8(&mut self, field: &str) -> Result<u8, String> {
        self.need(1, field)?;
        let v = self.bytes[self.pos];
        self.pos += 1;
        Ok(v)
    }

    #[inline]
    fn read_u32_le(&mut self, field: &str) -> Result<u32, String> {
        self.need(4, field)?;
        let v = u32::from_le_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    #[inline]
    fn read_u64_le(&mut self, field: &str) -> Result<u64, String> {
        self.need(8, field)?;
        let v = u64::from_le_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    #[inline]
    fn read_arr<const N: usize>(&mut self, field: &str) -> Result<[u8; N], String> {
        self.need(N, field)?;
        let v: [u8; N] = self.bytes[self.pos..self.pos + N].try_into().unwrap();
        self.pos += N;
        Ok(v)
    }
}

pub fn parse_header(bytes: &[u8]) -> Result<Header, String> {
    let mut r = Reader::new(bytes);

    let file_signature = r.read_arr::<4>("file_signature")?;
    let endianness_flag = r.read_u8("endianness_flag")?;
    let reserved_alignment = r.read_arr::<3>("reserved_alignment")?;

    if &file_signature != b"B000" {
        return Err("header: invalid file_signature (expected \"B000\")".into());
    }
    if endianness_flag != 0 {
        return Err("header: expected little-endian endianness_flag=0".into());
    }

    let off_spec_index = r.read_u64_le("off_spec_index")?;
    let off_chrom_index = r.read_u64_le("off_chrom_index")?;
    let off_spec_meta = r.read_u64_le("off_spec_meta")?;
    let off_chrom_meta = r.read_u64_le("off_chrom_meta")?;
    let off_global_meta = r.read_u64_le("off_global_meta")?;

    let size_container_spect_x = r.read_u64_le("size_container_spect_x")?;
    let off_container_spect_x = r.read_u64_le("off_container_spect_x")?;
    let size_container_spect_y = r.read_u64_le("size_container_spect_y")?;
    let off_container_spect_y = r.read_u64_le("off_container_spect_y")?;
    let size_container_chrom_x = r.read_u64_le("size_container_chrom_x")?;
    let off_container_chrom_x = r.read_u64_le("off_container_chrom_x")?;
    let size_container_chrom_y = r.read_u64_le("size_container_chrom_y")?;
    let off_container_chrom_y = r.read_u64_le("off_container_chrom_y")?;

    let spectrum_count = r.read_u32_le("spectrum_count")?;
    let chrom_count = r.read_u32_le("chrom_count")?;

    let spec_meta_count = r.read_u32_le("spec_meta_count")?;
    let spec_num_count = r.read_u32_le("spec_num_count")?;
    let spec_str_count = r.read_u32_le("spec_str_count")?;

    let chrom_meta_count = r.read_u32_le("chrom_meta_count")?;
    let chrom_num_count = r.read_u32_le("chrom_num_count")?;
    let chrom_str_count = r.read_u32_le("chrom_str_count")?;

    let global_meta_count = r.read_u32_le("global_meta_count")?;
    let global_num_count = r.read_u32_le("global_num_count")?;
    let global_str_count = r.read_u32_le("global_str_count")?;

    let block_count_spect_x = r.read_u32_le("block_count_spect_x")?;
    let block_count_spect_y = r.read_u32_le("block_count_spect_y")?;
    let block_count_chrom_x = r.read_u32_le("block_count_chrom_x")?;
    let block_count_chrom_y = r.read_u32_le("block_count_chrom_y")?;

    let reserved_flags = r.read_u8("reserved_flags")?;
    let chrom_x_format = r.read_u8("chrom_x_format")?;
    let chrom_y_format = r.read_u8("chrom_y_format")?;
    let spect_x_format = r.read_u8("spect_x_format")?;
    let spect_y_format = r.read_u8("spect_y_format")?;
    let compression_level = r.read_u8("compression_level")?;
    let array_filter = r.read_u8("array_filter")?;
    let reserved = r.read_arr::<13>("reserved")?;

    Ok(Header {
        file_signature,
        endianness_flag,
        reserved_alignment,

        off_spec_index,
        off_chrom_index,
        off_spec_meta,
        off_chrom_meta,
        off_global_meta,

        size_container_spect_x,
        off_container_spect_x,
        size_container_spect_y,
        off_container_spect_y,
        size_container_chrom_x,
        off_container_chrom_x,
        size_container_chrom_y,
        off_container_chrom_y,

        spectrum_count,
        chrom_count,

        spec_meta_count,
        spec_num_count,
        spec_str_count,

        chrom_meta_count,
        chrom_num_count,
        chrom_str_count,

        global_meta_count,
        global_num_count,
        global_str_count,

        block_count_spect_x,
        block_count_spect_y,
        block_count_chrom_x,
        block_count_chrom_y,

        reserved_flags,
        chrom_x_format,
        chrom_y_format,
        spect_x_format,
        spect_y_format,
        compression_level,
        array_filter,

        reserved,
    })
}

#[inline]
fn decompress_zstd(mut input: &[u8]) -> Result<Vec<u8>, String> {
    let mut dec = zstd::Decoder::new(&mut input).map_err(|e| format!("zstd decoder init: {e}"))?;
    let mut out = Vec::new();
    dec.read_to_end(&mut out)
        .map_err(|e| format!("zstd decode: {e}"))?;
    Ok(out)
}

#[inline]
fn decompress_zstd_allow_aligned_padding(input: &[u8]) -> Result<Vec<u8>, String> {
    match decompress_zstd(input) {
        Ok(v) => Ok(v),
        Err(first_err) => {
            let mut trimmed = input;
            for _ in 0..7 {
                if trimmed.is_empty() || *trimmed.last().unwrap() != 0 {
                    break;
                }
                trimmed = &trimmed[..trimmed.len() - 1];
                if let Ok(v) = decompress_zstd(trimmed) {
                    return Ok(v);
                }
            }
            Err(first_err)
        }
    }
}

#[inline]
fn unit_cv_ref(unit_accession: &Option<String>) -> Option<String> {
    unit_accession
        .as_deref()
        .and_then(|u| u.split_once(':'))
        .map(|(prefix, _)| prefix.to_string())
}
