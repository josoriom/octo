use std::{collections::HashSet, fs, path::PathBuf};

use crate::b64::decode2::{Metadatum, parse_binary_data_array_list, parse_header, parse_metadata};
use crate::mzml::schema::schema;

const PATH: &str = "data/b64/test.b64";

fn read_bytes(path: &str) -> Vec<u8> {
    let full = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
    fs::read(&full).unwrap_or_else(|e| panic!("cannot read {:?}: {}", full, e))
}

#[test]
fn check_binary_data_array_list_from_spectra_meta() {
    let bytes = read_bytes(PATH);
    let header = parse_header(&bytes).expect("parse_header failed");

    let spec_meta = parse_metadata_section_from_test_file(
        header.off_spec_meta,
        header.off_chrom_meta,
        header.spectrum_count,
        2,
        header.spec_meta_count,
        header.spec_num_count,
        header.spec_str_count,
        4,
        42,
        "spectra",
    );

    // ✅ schema() returns &'static SchemaTree
    let sc = schema();

    println!("--::>> Metadata:{:#?}", spec_meta);

    let bdal = parse_binary_data_array_list(sc, &spec_meta)
        .expect("parse_binary_data_array_list returned None");
    println!("--::>>{:#?}", bdal);
    // With your “one BinaryDataArray per item_index” rule, expect one per spectrum
    assert_eq!(bdal.count, Some(header.spectrum_count as usize));
    assert_eq!(
        bdal.binary_data_arrays.len(),
        header.spectrum_count as usize
    );

    // Validate: all cv_params are real CV prefixes (not B000 attributes)
    for (i, bda) in bdal.binary_data_arrays.iter().enumerate() {
        for p in &bda.cv_params {
            let acc = p.accession.as_deref().expect("cvParam missing accession");
            assert!(
                !acc.starts_with("B000:"),
                "item {i}: B000 attribute leaked into cv_params: {acc}"
            );
        }
    }

    // Spot-check first item has the expected BDA cv terms (based on your example)
    let bda0 = &bdal.binary_data_arrays[0];
    let accs: HashSet<&str> = bda0
        .cv_params
        .iter()
        .filter_map(|p| p.accession.as_deref())
        .collect();

    assert!(accs.contains("MS:1000522")); // 64-bit float
    assert!(accs.contains("MS:1000521")); // 32-bit float
    assert!(accs.contains("MS:1000576")); // no compression
    assert!(accs.contains("MS:1000514")); // m/z array
    assert!(accs.contains("MS:1000515")); // intensity array
}

fn parse_metadata_section_from_test_file(
    start_off: u64,
    end_off: u64,
    item_count: u32,
    expected_item_count: u32,
    meta_count: u32,
    num_count: u32,
    str_count: u32,
    compression_flag_bit: u8,
    expected_total_meta_len: usize,
    section_name: &str,
) -> Vec<Metadatum> {
    let bytes = read_bytes(PATH);
    let header = parse_header(&bytes).expect("parse_header failed");

    let c0 = start_off as usize;
    let c1 = end_off as usize;

    assert!(
        c0 < c1,
        "invalid metadata offsets for {section_name}: start >= end"
    );
    assert!(
        c1 <= bytes.len(),
        "invalid metadata offsets for {section_name}: end out of bounds"
    );

    assert_eq!(
        item_count, expected_item_count,
        "test.b64 should contain {expected_item_count} {section_name} items"
    );

    let compressed = (header.reserved_flags & (1u8 << compression_flag_bit)) != 0;
    let slice = &bytes[c0..c1];

    let meta = parse_metadata(
        slice,
        item_count,
        meta_count,
        num_count,
        str_count,
        compressed,
        header.reserved_flags,
    )
    .expect("parse_metadata failed");

    assert_eq!(
        meta.len(),
        expected_total_meta_len,
        "unexpected {section_name} metadata count (expected {expected_total_meta_len} total items)"
    );

    meta
}
