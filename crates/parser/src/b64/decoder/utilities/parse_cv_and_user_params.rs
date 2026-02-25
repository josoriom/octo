use crate::{
    CvParam, UserParam,
    b64::{
        attr_meta::CV_REF_ATTR,
        utilities::{
            // children_lookup::OwnerRows,
            common::{is_cv_prefix, unit_cv_ref, value_to_opt_string},
            cv_table,
        },
    },
    decoder::decode::{Metadatum, MetadatumValue},
    mzml::schema::TagId,
};

// #[inline]
// pub(crate) fn parse_list_grouped_by_owner_id<'m, T, F, I>(
//     iter: I,
//     mut parse_item: F,
// ) -> Option<Vec<T>>
// where
//     I: IntoIterator<Item = &'m Metadatum>,
//     F: FnMut(u32, &[&'m Metadatum]) -> T,
// {
//     let mut groups: OwnerRows<'m> = OwnerRows::new();

//     for entry in iter {
//         groups.insert(entry.id, entry);
//     }

//     if groups.is_empty() {
//         return None;
//     }

//     let mut entries: Vec<(u32, Vec<&'m Metadatum>)> = groups.into_iter().collect();
//     entries.sort_unstable_by_key(|(owner_id, _)| *owner_id);

//     let mut out = Vec::with_capacity(entries.len());
//     for (owner_id, group) in entries {
//         out.push(parse_item(owner_id, group.as_slice()));
//     }
//     Some(out)
// }

#[inline]
pub(crate) fn parse_cv_and_user_params(metadata: &[&Metadatum]) -> (Vec<CvParam>, Vec<UserParam>) {
    let mut cv_params = Vec::with_capacity(metadata.len());
    let mut user_params = Vec::new();

    for entry in metadata {
        if entry.tag_id == TagId::UserParam {
            user_params.push(parse_user_param(entry));
            continue;
        }

        let Some(accession) = entry.accession.as_deref() else {
            continue;
        };

        let Some((prefix, _)) = accession.split_once(':') else {
            continue;
        };

        if prefix == CV_REF_ATTR || !is_cv_prefix(prefix) {
            continue;
        }

        cv_params.push(parse_cv_param(entry, accession, prefix));
    }

    (cv_params, user_params)
}

#[inline]
fn parse_user_param(entry: &Metadatum) -> UserParam {
    let (name, value) = match &entry.value {
        MetadatumValue::Text(s) => match s.split_once('\0') {
            Some((name_part, value_part)) => {
                let value = if value_part.is_empty() {
                    None
                } else {
                    Some(value_part.to_string())
                };
                (name_part.to_string(), value)
            }
            None => (s.clone(), None),
        },
        MetadatumValue::Number(n) => (n.to_string(), None),
        MetadatumValue::Empty => (String::new(), None),
    };

    UserParam {
        name,
        value,
        r#type: None,
        unit_accession: entry.unit_accession.clone(),
        unit_cv_ref: unit_cv_ref(entry.unit_accession.as_deref()),
        unit_name: None,
    }
}

#[inline]
fn parse_cv_param(entry: &Metadatum, accession: &str, prefix: &str) -> CvParam {
    let unit_accession_str = entry.unit_accession.as_deref();

    let name = cv_table::get(accession)
        .and_then(|v| v.as_str())
        .unwrap_or(accession)
        .to_owned();

    let unit_name = unit_accession_str
        .and_then(|unit_acc| cv_table::get(unit_acc).and_then(|v| v.as_str()))
        .map(str::to_owned);

    CvParam {
        cv_ref: Some(prefix.to_owned()),
        accession: Some(accession.to_owned()),
        name,
        value: value_to_opt_string(&entry.value),
        unit_cv_ref: unit_cv_ref(unit_accession_str),
        unit_name,
        unit_accession: entry.unit_accession.clone(),
    }
}
