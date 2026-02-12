use crate::{
    b64::utilities::{
        children_lookup::{ChildrenLookup, OwnerRows},
        common::b000_attr_text,
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
    let mut owner_rows: OwnerRows<'_> = OwnerRows::with_capacity(metadata.len());

    let mut list_id: Option<u32> = None;
    let mut fallback_list_id: Option<u32> = None;

    for &m in metadata {
        owner_rows.entry(m.id).or_default().push(m);

        match m.tag_id {
            TagId::CvList => {
                list_id.get_or_insert(m.id);
            }
            TagId::Cv => {
                if fallback_list_id.is_none() && m.parent_index != 0 {
                    fallback_list_id = Some(m.parent_index);
                }
            }
            _ => {}
        }
    }

    let list_id = list_id.or(fallback_list_id)?;

    let cv_ids = children_lookup.ids_for(metadata, list_id, TagId::Cv);
    if cv_ids.is_empty() {
        return None;
    }

    let mut cv = Vec::with_capacity(cv_ids.len());
    for id in cv_ids {
        cv.push(parse_cv(&owner_rows, id));
    }

    Some(CvList {
        count: Some(cv.len()),
        cv,
    })
}

#[inline]
fn parse_cv(owner_rows: &OwnerRows, cv_id: u32) -> Cv {
    let rows = ChildrenLookup::rows_for_owner(owner_rows, cv_id);

    let id = b000_attr_text(rows, ACC_ATTR_LABEL)
        .or_else(|| b000_attr_text(rows, ACC_ATTR_ID))
        .unwrap_or_default();

    let full_name = b000_attr_text(rows, ACC_ATTR_CV_FULL_NAME).filter(|s| !s.is_empty());
    let version = b000_attr_text(rows, ACC_ATTR_CV_VERSION).filter(|s| !s.is_empty());
    let uri = b000_attr_text(rows, ACC_ATTR_CV_URI).filter(|s| !s.is_empty());

    Cv {
        id,
        full_name,
        version,
        uri,
    }
}
