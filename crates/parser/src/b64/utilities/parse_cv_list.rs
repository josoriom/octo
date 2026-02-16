use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::get_attr_text,
    },
    decode::Metadatum,
    mzml::{
        attr_meta::{
            ACC_ATTR_CV_FULL_NAME, ACC_ATTR_CV_URI, ACC_ATTR_CV_VERSION, ACC_ATTR_ID,
            ACC_ATTR_LABEL,
        },
        schema::TagId,
        structs::{Cv, CvList},
    },
};

#[inline]
pub fn parse_cv_list(metadata: &[&Metadatum], children_lookup: &ChildrenLookup) -> Option<CvList> {
    let mut owner_rows = OwnerRows::with_capacity(metadata.len());
    for &m in metadata {
        owner_rows.insert(m.id, m);
    }

    let list_id = children_lookup.all_ids(TagId::CvList).first().copied()?;
    let cv_ids = children_lookup.ids_for(list_id, TagId::Cv);

    if cv_ids.is_empty() {
        return None;
    }

    let cv = cv_ids
        .iter()
        .map(|&id| parse_cv(&owner_rows, id))
        .collect::<Vec<_>>();

    Some(CvList {
        count: Some(cv.len()),
        cv,
    })
}

#[inline]
fn parse_cv(owner_rows: &OwnerRows, cv_id: u32) -> Cv {
    let rows = owner_rows.get(cv_id);

    Cv {
        id: get_attr_text(rows, ACC_ATTR_ID)
            .or_else(|| get_attr_text(rows, ACC_ATTR_LABEL))
            .unwrap_or_default(),
        full_name: get_attr_text(rows, ACC_ATTR_CV_FULL_NAME).filter(|s| !s.is_empty()),
        version: get_attr_text(rows, ACC_ATTR_CV_VERSION).filter(|s| !s.is_empty()),
        uri: get_attr_text(rows, ACC_ATTR_CV_URI).filter(|s| !s.is_empty()),
    }
}
