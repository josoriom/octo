use crate::mzml::structs::CvParam;

pub(crate) const CV_REF_ATTR: &str = "B000";

pub(crate) const CV_CODE_MS: u8 = 0;
pub(crate) const CV_CODE_UO: u8 = 1;
pub(crate) const CV_CODE_NCIT: u8 = 2;
pub(crate) const CV_CODE_PEFF: u8 = 3;
pub(crate) const CV_CODE_B000: u8 = 4;
pub(crate) const CV_CODE_UNKNOWN: u8 = 255;

#[inline]
pub(crate) fn cv_ref_code_from_str(cv_ref: Option<&str>) -> u8 {
    match cv_ref {
        Some("MS") => CV_CODE_MS,
        Some("UO") => CV_CODE_UO,
        Some("NCIT") => CV_CODE_NCIT,
        Some("PEFF") => CV_CODE_PEFF,
        Some(CV_REF_ATTR) => CV_CODE_B000,
        _ => CV_CODE_UNKNOWN,
    }
}

#[inline]
pub(crate) fn cv_ref_prefix_from_code(code: u8) -> Option<&'static str> {
    match code {
        CV_CODE_MS => Some("MS"),
        CV_CODE_UO => Some("UO"),
        CV_CODE_NCIT => Some("NCIT"),
        CV_CODE_PEFF => Some("PEFF"),
        CV_CODE_B000 => Some(CV_REF_ATTR),
        _ => None,
    }
}

#[inline]
pub(crate) fn format_accession(cv_ref_code: u8, tail_raw: u32) -> Option<String> {
    let pref = cv_ref_prefix_from_code(cv_ref_code)?;
    match pref {
        "MS" => Some(format!(
            "MS:{:07}",
            normalize_ms_accession_tail(cv_ref_code, tail_raw)
        )),
        "UO" => Some(format!("UO:{tail_raw:07}")),
        "NCIT" => Some(format!("NCIT:C{tail_raw}")),
        x if x == CV_REF_ATTR => Some(format!("{CV_REF_ATTR}:{tail_raw}")),
        _ => Some(format!("{pref}:{tail_raw}")),
    }
}

const MS_ACCESSION_BASE: u32 = 1_000_000;

#[inline]
pub(crate) fn normalize_ms_accession_tail(cv_ref_code: u8, tail: u32) -> u32 {
    if cv_ref_code == CV_CODE_MS && tail != 0 && tail < MS_ACCESSION_BASE {
        MS_ACCESSION_BASE + tail
    } else {
        tail
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct AccessionTail(u32);

impl std::fmt::Display for AccessionTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AccessionTail {
    #[inline]
    pub(crate) fn raw(self) -> u32 {
        self.0
    }

    #[inline]
    pub(crate) fn from_raw(tail: u32) -> Self {
        Self(tail)
    }
}

#[inline]
pub(crate) fn parse_accession_tail(accession: Option<&str>) -> AccessionTail {
    let s = accession.unwrap_or("");
    let tail = s.rsplit_once(':').map(|(_, t)| t).unwrap_or(s);
    let mut v: u32 = 0;
    let mut saw = false;
    for b in tail.bytes() {
        if b.is_ascii_digit() {
            saw = true;
            v = match v
                .checked_mul(10)
                .and_then(|x| x.checked_add((b - b'0') as u32))
            {
                Some(n) => n,
                None => return AccessionTail::from_raw(0),
            };
        }
    }
    AccessionTail::from_raw(if saw { v } else { 0 })
}

// ── Attribute accession tail constants ───────────────────────────────────────

// String-valued attributes
pub(crate) const ACC_ATTR_ID: AccessionTail = AccessionTail(9_910_001);
pub(crate) const ACC_ATTR_REF: AccessionTail = AccessionTail(9_910_002);
pub(crate) const ACC_ATTR_NAME: AccessionTail = AccessionTail(9_910_003);
pub(crate) const ACC_ATTR_LOCATION: AccessionTail = AccessionTail(9_910_004);

// <cv> element attributes
pub(crate) const ACC_ATTR_CV_ID: AccessionTail = AccessionTail(9_900_001);
pub(crate) const ACC_ATTR_CV_FULL_NAME: AccessionTail = AccessionTail(9_900_002);
pub(crate) const ACC_ATTR_CV_VERSION: AccessionTail = AccessionTail(9_900_003);
pub(crate) const ACC_ATTR_CV_URI: AccessionTail = AccessionTail(9_900_004);
pub(crate) const ACC_ATTR_LABEL: AccessionTail = AccessionTail(9_910_020);

// Run / section references
pub(crate) const ACC_ATTR_START_TIME_STAMP: AccessionTail = AccessionTail(9_910_005);
pub(crate) const ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF: AccessionTail =
    AccessionTail(9_910_006);
pub(crate) const ACC_ATTR_DEFAULT_SOURCE_FILE_REF: AccessionTail = AccessionTail(9_910_007);
pub(crate) const ACC_ATTR_SAMPLE_REF: AccessionTail = AccessionTail(9_910_008);

// Data processing / source file
pub(crate) const ACC_ATTR_DEFAULT_DATA_PROCESSING_REF: AccessionTail = AccessionTail(9_910_009);
pub(crate) const ACC_ATTR_DATA_PROCESSING_REF: AccessionTail = AccessionTail(9_910_010);
pub(crate) const ACC_ATTR_SOURCE_FILE_REF: AccessionTail = AccessionTail(9_910_011);

// Spectrum identity
pub(crate) const ACC_ATTR_NATIVE_ID: AccessionTail = AccessionTail(9_910_012);
pub(crate) const ACC_ATTR_SPOT_ID: AccessionTail = AccessionTail(9_910_013);
pub(crate) const ACC_ATTR_EXTERNAL_SPECTRUM_ID: AccessionTail = AccessionTail(9_910_014);
pub(crate) const ACC_ATTR_SPECTRUM_REF: AccessionTail = AccessionTail(9_910_015);

// Instrument / scan settings
pub(crate) const ACC_ATTR_SCAN_SETTINGS_REF: AccessionTail = AccessionTail(9_910_016);
pub(crate) const ACC_ATTR_INSTRUMENT_CONFIGURATION_REF: AccessionTail = AccessionTail(9_910_017);

// Software
pub(crate) const ACC_ATTR_SOFTWARE_REF: AccessionTail = AccessionTail(9_910_018);
pub(crate) const ACC_ATTR_VERSION: AccessionTail = AccessionTail(9_910_019);

// Numeric-valued
pub(crate) const ACC_ATTR_COUNT: AccessionTail = AccessionTail(9_910_100);
pub(crate) const ACC_ATTR_ORDER: AccessionTail = AccessionTail(9_910_101);
pub(crate) const ACC_ATTR_INDEX: AccessionTail = AccessionTail(9_910_102);
pub(crate) const ACC_ATTR_SCAN_NUMBER: AccessionTail = AccessionTail(9_910_103);
pub(crate) const ACC_ATTR_DEFAULT_ARRAY_LENGTH: AccessionTail = AccessionTail(9_910_104);
pub(crate) const ACC_ATTR_ARRAY_LENGTH: AccessionTail = AccessionTail(9_910_105);
pub(crate) const ACC_ATTR_ENCODED_LENGTH: AccessionTail = AccessionTail(9_910_106);
pub(crate) const ACC_ATTR_MS_LEVEL: AccessionTail = AccessionTail(9_910_107);

#[inline]
pub(crate) fn attr_cv_param(tail: AccessionTail, value: &str) -> CvParam {
    CvParam {
        cv_ref: Some(CV_REF_ATTR.to_string()),
        accession: Some(format!("{}:{:07}", CV_REF_ATTR, tail.raw())),
        name: String::new(),
        value: Some(value.to_string()),
        unit_cv_ref: None,
        unit_name: None,
        unit_accession: None,
    }
}

#[inline]
pub(crate) fn key_to_attr_tail(key: &str) -> Option<AccessionTail> {
    Some(match key {
        "id" => ACC_ATTR_ID,
        "ref" => ACC_ATTR_REF,
        "name" => ACC_ATTR_NAME,
        "location" => ACC_ATTR_LOCATION,
        "cvID" => ACC_ATTR_CV_ID,
        "fullName" => ACC_ATTR_CV_FULL_NAME,
        "version" => ACC_ATTR_VERSION,
        "URI" => ACC_ATTR_CV_URI,
        "label" => ACC_ATTR_LABEL,
        "startTimeStamp" => ACC_ATTR_START_TIME_STAMP,
        "defaultInstrumentConfigurationRef" => ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
        "defaultSourceFileRef" => ACC_ATTR_DEFAULT_SOURCE_FILE_REF,
        "sampleRef" => ACC_ATTR_SAMPLE_REF,
        "defaultDataProcessingRef" => ACC_ATTR_DEFAULT_DATA_PROCESSING_REF,
        "dataProcessingRef" => ACC_ATTR_DATA_PROCESSING_REF,
        "sourceFileRef" => ACC_ATTR_SOURCE_FILE_REF,
        "nativeID" => ACC_ATTR_NATIVE_ID,
        "spotID" => ACC_ATTR_SPOT_ID,
        "externalSpectrumID" => ACC_ATTR_EXTERNAL_SPECTRUM_ID,
        "spectrumRef" => ACC_ATTR_SPECTRUM_REF,
        "scanSettingsRef" => ACC_ATTR_SCAN_SETTINGS_REF,
        "instrumentConfigurationRef" => ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
        "softwareRef" => ACC_ATTR_SOFTWARE_REF,
        "count" => ACC_ATTR_COUNT,
        "order" => ACC_ATTR_ORDER,
        "index" => ACC_ATTR_INDEX,
        "scanNumber" => ACC_ATTR_SCAN_NUMBER,
        "defaultArrayLength" => ACC_ATTR_DEFAULT_ARRAY_LENGTH,
        "arrayLength" => ACC_ATTR_ARRAY_LENGTH,
        "encodedLength" => ACC_ATTR_ENCODED_LENGTH,
        "msLevel" => ACC_ATTR_MS_LEVEL,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_maps(key: &str, expected: AccessionTail) {
        assert_eq!(
            key_to_attr_tail(key),
            Some(expected),
            "key_to_attr_tail({key:?}) should be {expected:?}"
        );
    }

    #[test]
    fn key_id() {
        assert_maps("id", ACC_ATTR_ID);
    }
    #[test]
    fn key_ref() {
        assert_maps("ref", ACC_ATTR_REF);
    }
    #[test]
    fn key_name() {
        assert_maps("name", ACC_ATTR_NAME);
    }
    #[test]
    fn key_location() {
        assert_maps("location", ACC_ATTR_LOCATION);
    }
    #[test]
    fn key_cv_id() {
        assert_maps("cvID", ACC_ATTR_CV_ID);
    }
    #[test]
    fn key_full_name() {
        assert_maps("fullName", ACC_ATTR_CV_FULL_NAME);
    }
    #[test]
    fn key_version() {
        assert_maps("version", ACC_ATTR_VERSION);
    }
    #[test]
    fn key_uri() {
        assert_maps("URI", ACC_ATTR_CV_URI);
    }
    #[test]
    fn key_label() {
        assert_maps("label", ACC_ATTR_LABEL);
    }
    #[test]
    fn key_start_time_stamp() {
        assert_maps("startTimeStamp", ACC_ATTR_START_TIME_STAMP);
    }
    #[test]
    fn key_default_instrument_configuration_ref() {
        assert_maps(
            "defaultInstrumentConfigurationRef",
            ACC_ATTR_DEFAULT_INSTRUMENT_CONFIGURATION_REF,
        );
    }
    #[test]
    fn key_default_source_file_ref() {
        assert_maps("defaultSourceFileRef", ACC_ATTR_DEFAULT_SOURCE_FILE_REF);
    }
    #[test]
    fn key_sample_ref() {
        assert_maps("sampleRef", ACC_ATTR_SAMPLE_REF);
    }
    #[test]
    fn key_default_data_processing_ref() {
        assert_maps(
            "defaultDataProcessingRef",
            ACC_ATTR_DEFAULT_DATA_PROCESSING_REF,
        );
    }
    #[test]
    fn key_data_processing_ref() {
        assert_maps("dataProcessingRef", ACC_ATTR_DATA_PROCESSING_REF);
    }
    #[test]
    fn key_source_file_ref() {
        assert_maps("sourceFileRef", ACC_ATTR_SOURCE_FILE_REF);
    }
    #[test]
    fn key_native_id() {
        assert_maps("nativeID", ACC_ATTR_NATIVE_ID);
    }
    #[test]
    fn key_spot_id() {
        assert_maps("spotID", ACC_ATTR_SPOT_ID);
    }
    #[test]
    fn key_external_spectrum_id() {
        assert_maps("externalSpectrumID", ACC_ATTR_EXTERNAL_SPECTRUM_ID);
    }
    #[test]
    fn key_spectrum_ref() {
        assert_maps("spectrumRef", ACC_ATTR_SPECTRUM_REF);
    }
    #[test]
    fn key_scan_settings_ref() {
        assert_maps("scanSettingsRef", ACC_ATTR_SCAN_SETTINGS_REF);
    }
    #[test]
    fn key_instrument_configuration_ref() {
        assert_maps(
            "instrumentConfigurationRef",
            ACC_ATTR_INSTRUMENT_CONFIGURATION_REF,
        );
    }
    #[test]
    fn key_software_ref() {
        assert_maps("softwareRef", ACC_ATTR_SOFTWARE_REF);
    }
    #[test]
    fn key_count() {
        assert_maps("count", ACC_ATTR_COUNT);
    }
    #[test]
    fn key_order() {
        assert_maps("order", ACC_ATTR_ORDER);
    }
    #[test]
    fn key_index() {
        assert_maps("index", ACC_ATTR_INDEX);
    }
    #[test]
    fn key_scan_number() {
        assert_maps("scanNumber", ACC_ATTR_SCAN_NUMBER);
    }
    #[test]
    fn key_default_array_length() {
        assert_maps("defaultArrayLength", ACC_ATTR_DEFAULT_ARRAY_LENGTH);
    }
    #[test]
    fn key_array_length() {
        assert_maps("arrayLength", ACC_ATTR_ARRAY_LENGTH);
    }
    #[test]
    fn key_encoded_length() {
        assert_maps("encodedLength", ACC_ATTR_ENCODED_LENGTH);
    }
    #[test]
    fn key_ms_level() {
        assert_maps("msLevel", ACC_ATTR_MS_LEVEL);
    }
    #[test]
    fn unknown_key_returns_none() {
        assert_eq!(key_to_attr_tail("unknownFutureKey"), None);
    }

    #[test]
    fn parse_accession_tail_roundtrip() {
        assert_eq!(parse_accession_tail(Some("MS:1000514")).raw(), 1_000_514);
        assert_eq!(parse_accession_tail(Some("B000:9910001")).raw(), 9_910_001);
        assert_eq!(parse_accession_tail(None).raw(), 0);
        assert_eq!(parse_accession_tail(Some("no-colon")).raw(), 0);
    }

    #[test]
    fn attr_cv_param_round_trip() {
        let cv = attr_cv_param(ACC_ATTR_ID, "scan=1");
        assert_eq!(cv.cv_ref.as_deref(), Some(CV_REF_ATTR));
        assert_eq!(cv.value.as_deref(), Some("scan=1"));
        assert!(cv.accession.as_deref().unwrap().contains("9910001"));
    }

    #[test]
    fn cv_ref_code_round_trip() {
        for code in [
            CV_CODE_MS,
            CV_CODE_UO,
            CV_CODE_NCIT,
            CV_CODE_PEFF,
            CV_CODE_B000,
        ] {
            let prefix = cv_ref_prefix_from_code(code).unwrap();
            assert_eq!(cv_ref_code_from_str(Some(prefix)), code);
        }
    }
}
