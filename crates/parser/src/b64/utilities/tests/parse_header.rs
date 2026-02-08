use std::{fs, path::PathBuf};

use crate::b64::utilities::parse_header;

const PATH: &str = "data/b64/test.b64";

fn read_bytes(path: &str) -> Vec<u8> {
    let full = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
    fs::read(&full).unwrap_or_else(|e| panic!("cannot read {:?}: {}", full, e))
}

#[test]
fn check_header() {
    let bytes = read_bytes(PATH);
    let header = parse_header(&bytes).expect("parse_header failed");

    assert_eq!(header.file_signature, [66, 48, 48, 48]);
    assert_eq!(header.endianness_flag, 0);
    assert_eq!(header.reserved_alignment, [0, 0, 0]);

    assert_eq!(header.spectrum_count, 2);
    assert_eq!(header.chrom_count, 2);

    assert_eq!(header.spec_meta_count, 54);
    assert_eq!(header.spec_num_count, 32);
    assert_eq!(header.spec_str_count, 3);

    assert_eq!(header.chrom_meta_count, 38);
    assert_eq!(header.chrom_num_count, 15);
    assert_eq!(header.chrom_str_count, 5);

    assert_eq!(header.global_meta_count, 49);
    assert_eq!(header.global_num_count, 8);
    assert_eq!(header.global_str_count, 25);

    assert_eq!(header.block_count_spect, 4);
    assert_eq!(header.block_count_chrom, 5);

    assert_eq!(header.codec_id, 1);
    assert_eq!(header.compression_level, 12);
    assert_eq!(header.array_filter, 1);

    assert_eq!(header.len_spec_entries, (header.spectrum_count as u64) * 32);
    assert_eq!(header.len_chrom_entries, (header.chrom_count as u64) * 32);

    assert_eq!(header.len_spec_entries % 32, 0);
    assert_eq!(header.len_spec_arrayrefs % 32, 0);
    assert_eq!(header.len_chrom_entries % 32, 0);
    assert_eq!(header.len_chrom_arrayrefs % 32, 0);

    assert!(header.len_container_spect >= (header.block_count_spect as u64) * 32);
    assert!(header.len_container_chrom >= (header.block_count_chrom as u64) * 32);

    assert!(header.size_spec_meta_uncompressed > 0);
    assert!(header.size_chrom_meta_uncompressed > 0);
    assert!(header.size_global_meta_uncompressed > 0);

    let len = bytes.len() as u64;

    let segments = vec![
        (
            "spec_entries",
            header.off_spec_entries,
            header.len_spec_entries,
        ),
        (
            "spec_arrayrefs",
            header.off_spec_arrayrefs,
            header.len_spec_arrayrefs,
        ),
        (
            "chrom_entries",
            header.off_chrom_entries,
            header.len_chrom_entries,
        ),
        (
            "chrom_arrayrefs",
            header.off_chrom_arrayrefs,
            header.len_chrom_arrayrefs,
        ),
        ("spec_meta", header.off_spec_meta, header.len_spec_meta),
        ("chrom_meta", header.off_chrom_meta, header.len_chrom_meta),
        (
            "global_meta",
            header.off_global_meta,
            header.len_global_meta,
        ),
        (
            "container_spect",
            header.off_container_spect,
            header.len_container_spect,
        ),
        (
            "container_chrom",
            header.off_container_chrom,
            header.len_container_chrom,
        ),
    ];

    for (name, off, seg_len) in &segments {
        assert!(
            *off < len,
            "{name}: off out of bounds (off={off}, file_len={len})"
        );
        let end = off + seg_len;
        assert!(
            end <= len,
            "{name}: end out of bounds (end={end}, file_len={len})"
        );
    }

    for w in segments.windows(2) {
        let (a_name, a_off, a_len) = w[0];
        let (b_name, b_off, _) = w[1];
        let a_end = a_off + a_len;
        assert!(
            b_off >= a_end,
            "overlap/out-of-order: {a_name} ends at {a_end}, {b_name} starts at {b_off}"
        );
    }
}
