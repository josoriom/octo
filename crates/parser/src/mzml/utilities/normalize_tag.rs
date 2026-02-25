// ── legacy: tag normalisation ────────────────────────
// All legacy-alias lives here.
/// mzML 1.1 equivalent.  Returns the input unchanged when no alias exists.
#[inline]
pub fn normalize_tag(s: &str) -> &str {
    match s {
        "paramGroupRef" => "referenceableParamGroupRef",
        "instrumentSoftwareRef" => "softwareRef",
        "instrument" => "instrumentConfiguration",
        "instrumentList" => "instrumentConfigurationList",
        "acquisitionSettingsList" => "scanSettingsList",
        "acquisitionSettings" => "scanSettings",
        "acquisitionList" => "scanList",
        "acquisition" => "scan",
        "selectionWindowList" => "scanWindowList",
        "selectionWindow" => "scanWindow",
        "ionSelection" => "selectedIon",
        other => other,
    }
}
