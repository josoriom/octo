use crate::mzml::utilities::normalize_tag::normalize_tag;

fn round_trips(canonical: &str) {
    assert_eq!(normalize_tag(canonical), canonical);
}

#[test]
fn alias_param_group_ref() {
    assert_eq!(normalize_tag("paramGroupRef"), "referenceableParamGroupRef");
}
#[test]
fn alias_instrument_software_ref() {
    assert_eq!(normalize_tag("instrumentSoftwareRef"), "softwareRef");
}
#[test]
fn alias_instrument() {
    assert_eq!(normalize_tag("instrument"), "instrumentConfiguration");
}
#[test]
fn alias_instrument_list() {
    assert_eq!(
        normalize_tag("instrumentList"),
        "instrumentConfigurationList"
    );
}
#[test]
fn alias_acquisition_settings_list() {
    assert_eq!(normalize_tag("acquisitionSettingsList"), "scanSettingsList");
}
#[test]
fn alias_acquisition_settings() {
    assert_eq!(normalize_tag("acquisitionSettings"), "scanSettings");
}
#[test]
fn alias_acquisition_list() {
    assert_eq!(normalize_tag("acquisitionList"), "scanList");
}
#[test]
fn alias_acquisition() {
    assert_eq!(normalize_tag("acquisition"), "scan");
}
#[test]
fn alias_selection_window_list() {
    assert_eq!(normalize_tag("selectionWindowList"), "scanWindowList");
}
#[test]
fn alias_selection_window() {
    assert_eq!(normalize_tag("selectionWindow"), "scanWindow");
}
#[test]
fn alias_ion_selection() {
    assert_eq!(normalize_tag("ionSelection"), "selectedIon");
}
#[test]
fn canonical_pass_through() {
    round_trips("scanList");
    round_trips("spectrum");
    round_trips("cvParam");
}
#[test]
fn unknown_pass_through() {
    assert_eq!(normalize_tag("unknownFutureTag"), "unknownFutureTag");
}
