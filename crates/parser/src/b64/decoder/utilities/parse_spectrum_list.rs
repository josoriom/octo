use crate::{
    Spectrum, SpectrumDescription, SpectrumList,
    b64::{
        attr_meta::{
            ACC_ATTR_COUNT, ACC_ATTR_DATA_PROCESSING_REF, ACC_ATTR_DEFAULT_ARRAY_LENGTH,
            ACC_ATTR_DEFAULT_DATA_PROCESSING_REF, ACC_ATTR_ID, ACC_ATTR_INDEX, ACC_ATTR_NATIVE_ID,
            ACC_ATTR_SCAN_NUMBER, ACC_ATTR_SOURCE_FILE_REF, ACC_ATTR_SPOT_ID,
        },
        utilities::{
            children_lookup::{ChildrenLookup, MetadataPolicy, OwnerRows},
            common::{get_attr_text, get_attr_u32, xy_lengths_from_bdal},
            parse_binary_data_array_list, parse_cv_and_user_params, parse_precursor_list,
            parse_product_list, parse_scan_list,
        },
    },
    decoder::decode::Metadatum,
    mzml::schema::TagId,
};

const ACC_MS_LEVEL: &str = "MS:1000511";

#[inline]
pub(crate) fn parse_spectrum_list<P: MetadataPolicy>(
    metadata: &[&Metadatum],
    children_lookup: &ChildrenLookup,
    policy: &P,
) -> Option<SpectrumList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &entry in metadata {
        owner_rows.insert(entry.id, entry);
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
    let default_data_processing_ref =
        get_attr_text(list_rows, ACC_ATTR_DEFAULT_DATA_PROCESSING_REF);

    let mut param_buffer: Vec<&Metadatum> = Vec::new();

    let spectra: Vec<Spectrum> = spectrum_ids
        .iter()
        .enumerate()
        .map(|(index, &spectrum_id)| {
            parse_spectrum(
                &owner_rows,
                children_lookup,
                spectrum_id,
                index as u32,
                default_data_processing_ref.as_deref(),
                policy,
                &mut param_buffer,
            )
        })
        .collect();

    Some(SpectrumList {
        count: count.or(Some(spectra.len())),
        default_data_processing_ref,
        spectra,
    })
}

#[inline]
fn parse_spectrum<'a, P: MetadataPolicy>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
    fallback_index: u32,
    default_data_processing_ref: Option<&str>,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Spectrum {
    let rows = owner_rows.get(spectrum_id);

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, spectrum_id, policy, param_buffer);
    let (mut cv_params, mut user_params) = parse_cv_and_user_params(param_buffer);

    let spectrum_description = parse_description(
        owner_rows,
        children_lookup,
        spectrum_id,
        policy,
        param_buffer,
    );

    if let Some(description) = &spectrum_description {
        cv_params.retain(|p| {
            !description
                .cv_params
                .iter()
                .any(|dp| p.accession == dp.accession)
        });
        user_params.retain(|p| !description.user_params.iter().any(|dp| p.name == dp.name));
    }

    let ms_level = cv_params
        .iter()
        .find(|p| p.accession.as_deref() == Some(ACC_MS_LEVEL))
        .and_then(|p| p.value.as_deref())
        .and_then(|v| v.parse::<u32>().ok());

    let binary_data_array_list =
        parse_binary_data_array_list(owner_rows, children_lookup, spectrum_id);
    let (x_len, y_len) = xy_lengths_from_bdal(binary_data_array_list.as_ref());

    Spectrum {
        id: get_attr_text(rows, ACC_ATTR_ID).unwrap_or_default(),
        index: get_attr_u32(rows, ACC_ATTR_INDEX).or(Some(fallback_index)),
        scan_number: get_attr_u32(rows, ACC_ATTR_SCAN_NUMBER),
        default_array_length: get_attr_u32(rows, ACC_ATTR_DEFAULT_ARRAY_LENGTH)
            .map(|v| v as usize)
            .or(x_len)
            .or(y_len)
            .or(Some(0)),
        native_id: get_attr_text(rows, ACC_ATTR_NATIVE_ID),
        data_processing_ref: get_attr_text(rows, ACC_ATTR_DATA_PROCESSING_REF)
            .or_else(|| default_data_processing_ref.map(ToString::to_string)),
        source_file_ref: get_attr_text(rows, ACC_ATTR_SOURCE_FILE_REF),
        spot_id: get_attr_text(rows, ACC_ATTR_SPOT_ID),
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
fn parse_description<'a, P: MetadataPolicy>(
    owner_rows: &'a OwnerRows<'a>,
    children_lookup: &ChildrenLookup,
    spectrum_id: u32,
    policy: &P,
    param_buffer: &mut Vec<&'a Metadatum>,
) -> Option<SpectrumDescription> {
    let description_id = children_lookup
        .ids_for(spectrum_id, TagId::SpectrumDescription)
        .first()
        .copied()?;

    param_buffer.clear();
    children_lookup.get_param_rows_into(owner_rows, description_id, policy, param_buffer);
    let (cv_params, user_params) = parse_cv_and_user_params(param_buffer);

    Some(SpectrumDescription {
        referenceable_param_group_refs: Vec::new(),
        cv_params,
        user_params,
        scan_list: parse_scan_list(owner_rows, children_lookup, description_id),
        precursor_list: parse_precursor_list(owner_rows, children_lookup, description_id),
        product_list: parse_product_list(owner_rows, children_lookup, description_id),
    })
}
