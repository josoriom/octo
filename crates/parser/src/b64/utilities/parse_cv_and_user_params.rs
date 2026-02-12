use crate::{
    CvParam, UserParam,
    b64::utilities::{
        children_lookup::OwnerRows,
        common::{is_cv_prefix, unit_cv_ref, value_to_opt_string},
    },
    decode::Metadatum,
    mzml::{attr_meta::CV_REF_ATTR, cv_table},
};

#[inline]
pub fn parse_list_grouped_by_owner_id<'m, T, F, I>(iter: I, mut parse_item: F) -> Option<Vec<T>>
where
    I: IntoIterator<Item = &'m Metadatum>,
    F: FnMut(u32, &[&'m Metadatum]) -> T,
{
    let mut groups: OwnerRows<'m> = OwnerRows::new();

    for m in iter {
        groups.entry(m.id).or_default().push(m);
    }

    if groups.is_empty() {
        return None;
    }

    let mut entries: Vec<(u32, Vec<&'m Metadatum>)> = groups.into_iter().collect();
    entries.sort_unstable_by_key(|(id, _)| *id);

    let mut out = Vec::with_capacity(entries.len());
    for (id, group) in entries {
        out.push(parse_item(id, group.as_slice()));
    }
    Some(out)
}

#[inline]
pub fn parse_cv_and_user_params(metadata: &[&Metadatum]) -> (Vec<CvParam>, Vec<UserParam>) {
    let mut cv_params = Vec::with_capacity(metadata.len());
    let mut user_params = Vec::new();

    for m in metadata {
        let Some(acc) = m.accession.as_deref() else {
            continue;
        };
        let Some((prefix, _)) = acc.split_once(':') else {
            continue;
        };

        if prefix == CV_REF_ATTR {
            continue;
        }

        let value = value_to_opt_string(&m.value);

        let unit_accession_str = m.unit_accession.as_deref();
        let unit_cv_ref = unit_cv_ref(unit_accession_str);

        let name = cv_table::get(acc)
            .and_then(|v| v.as_str())
            .unwrap_or(acc)
            .to_owned();

        let unit_name = unit_accession_str
            .and_then(|ua| cv_table::get(ua).and_then(|v| v.as_str()))
            .map(str::to_owned);

        let unit_accession = m.unit_accession.clone();

        if is_cv_prefix(prefix) {
            cv_params.push(CvParam {
                cv_ref: Some(prefix.to_owned()),
                accession: Some(acc.to_owned()),
                name,
                value,
                unit_cv_ref,
                unit_name,
                unit_accession,
            });
        } else {
            user_params.push(UserParam {
                name,
                r#type: None,
                unit_accession,
                unit_cv_ref,
                unit_name: None, // keep existing behavior (don’t populate unit_name for user params)
                value,
            });
        }
    }

    (cv_params, user_params)
}
