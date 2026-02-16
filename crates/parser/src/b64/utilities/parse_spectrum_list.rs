use crate::{
    Spectrum, SpectrumDescription, SpectrumList,
    b64::{
        decode::Metadatum,
        utilities::{
            children_lookup::{ChildrenLookup, OwnerRows},
            common::{get_attr_text, get_attr_u32, xy_lengths_from_bdal},
            parse_binary_data_array_list::parse_binary_data_array_list,
            parse_cv_and_user_params,
            parse_precursor_list::parse_precursor_list,
            parse_product_list::parse_product_list,
            parse_scan_list::parse_scan_list,
        },
    },
    mzml::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF, ACC_ATTR_DEFAULT_ARRAY_LENGTH,
            ACC_ATTR_DEFAULT_DATA_PROCESSING_REF, ACC_ATTR_ID, ACC_ATTR_INDEX, ACC_ATTR_NATIVE_ID,
            ACC_ATTR_SCAN_NUMBER, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPOT_ID,
        },
        schema::TagId,
    },
};

#[inline]
pub fn parse_spectrum_list(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
) -> Option<SpectrumList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &row in metadata {
        owner_rows.insert(row.id, row);
    }

    let list_id = children_lookup
        .all_ids(TagId::SpectrumList)
        .first()
        .copied()?;

    let spectrum_ids = children_lookup.ids_for(list_id, TagId::Spectrum);
    if spectrum_ids.is_empty() {
        return None;
    }

    let list_rows = owner_rows.get(list_id);
    let count = get_attr_u32(list_rows, ACC_ATTR_COUNT).map(|v| v as usize);
    let default_dp_ref = get_attr_text(list_rows, ACC_ATTR_DEFAULT_DATA_PROCESSING_REF);

    let spectra: Vec<Spectrum> = spectrum_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            parse_spectrum(
                &owner_rows,
                children_lookup,
                id,
                i as u32,
                default_dp_ref.as_deref(),
            )
        })
        .collect();

    Some(SpectrumList {
        count: count.or(Some(spectra.len())),
        default_data_processing_ref: default_dp_ref,
        spectra,
    })
}

#[inline]
fn parse_spectrum(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
    index_fallback: u32,
    default_dp_ref: Option<&str>,
) -> Spectrum {
    let rows = owner_rows.get(spectrum_id);

    let param_rows = &children_lookup.get_param_rows(owner_rows, spectrum_id);
    let (mut cv_params, mut user_params) = parse_cv_and_user_params(&param_rows);

    let spectrum_description = parse_description(owner_rows, children_lookup, spectrum_id);

    if let Some(desc) = &spectrum_description {
        cv_params.retain(|p| !desc.cv_params.iter().any(|dp| p.accession == dp.accession));
        user_params.retain(|p| !desc.user_params.iter().any(|dp| p.name == dp.name));
    }

    let id = get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default();
    let index = get_attr_u32(rows, ACC_ATTR_INDEX).or(Some(index_fallback));
    let native_id = get_attr_text(rows, ACC_ATTR_NATIVE_ID);
    let spot_id = get_attr_text(rows, ACC_ATTR_SPOT_ID);
    let source_file_ref = get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF);
    let scan_number = get_attr_u32(rows, ACC_ATTR_SCAN_NUMBER);

    let data_processing_ref = get_attr_text(rows, ACC_ATTR_DATA_PROCESSING_REF)
        .or_else(|| default_dp_ref.map(ToString::to_string));

    let ms_level = cv_params
        .iter()
        .find(|p| p.accession.as_deref() == Some("MS:1000511"))
        .and_then(|p| p.value.as_deref())
        .and_then(|v| v.parse::<u32>().ok());

    let binary_data_array_list =
        parse_binary_data_array_list(owner_rows, children_lookup, spectrum_id);

    let (x_len, y_len) = xy_lengths_from_bdal(binary_data_array_list.as_ref());
    let default_array_length = get_attr_u32(rows, ACC_ATTR_DEFAULT_ARRAY_LENGTH)
        .map(|v| v as usize)
        .or(x_len)
        .or(y_len)
        .or(Some(0));

    Spectrum {
        id,
        index,
        scan_number,
        default_array_length,
        native_id,
        data_processing_ref,
        source_file_ref,
        spot_id,
        ms_level,
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        spectrum_description,
        scan_list: parse_scan_list(owner_rows, children_lookup, spectrum_id),
        precursor_list: parse_precursor_list(owner_rows, children_lookup, spectrum_id),
        product_list: parse_product_list(owner_rows, children_lookup, spectrum_id),
        binary_data_array_list,
    }
}

#[inline]
fn parse_description(
    owner_rows: &OwnerRows,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
) -> Option<SpectrumDescription> {
    let desc_id = *children_lookup
        .ids_for(spectrum_id, TagId::SpectrumDescription)
        .first()?;

    let param_rows = &children_lookup.get_param_rows(owner_rows, desc_id);
    let (cv_params, user_params) = parse_cv_and_user_params(&param_rows);

    Some(SpectrumDescription {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        scan_list: None,
        precursor_list: None,
        product_list: None,
    })
}
