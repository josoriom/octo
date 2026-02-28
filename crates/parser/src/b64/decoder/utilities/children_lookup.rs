use crate::{decoder::decode::Metadatum, mzml::schema::TagId};
use hashbrown::{HashMap as HbHashMap, HashSet as HbHashSet, hash_map};

type Map<K, V> = HbHashMap<K, V>;

const EXCLUDED_ACCESSION_PREFIX: &str = "B000:";

pub(crate) trait MetadataPolicy {
    fn is_param(&self, tag: TagId) -> bool;
    fn should_exclude(&self, m: &Metadatum) -> bool;
    fn traversal_tags(&self) -> &[TagId];
}

#[repr(transparent)]
pub(crate) struct OwnerRows<'m>(Map<u32, Vec<&'m Metadatum>>);

impl<'m> OwnerRows<'m> {
    #[inline]
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self(Map::with_capacity(cap))
    }

    #[inline]
    pub(crate) fn get(&self, id: u32) -> &[&'m Metadatum] {
        self.0.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    #[inline]
    pub(crate) fn insert(&mut self, id: u32, row: &'m Metadatum) {
        self.0.entry(id).or_default().push(row);
    }
}

impl<'m> IntoIterator for OwnerRows<'m> {
    type Item = (u32, Vec<&'m Metadatum>);
    type IntoIter = hash_map::IntoIter<u32, Vec<&'m Metadatum>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub(crate) struct ChildrenLookup {
    ids_by_parent_tag: Map<u64, Vec<u32>>,
    ids_by_tag: Map<TagId, Vec<u32>>,
}

impl ChildrenLookup {
    pub(crate) fn new(metadata: &[Metadatum]) -> Self {
        let mut capacity_by_parent_tag: Map<u64, usize> = Map::new();
        let mut capacity_by_tag: Map<TagId, usize> = Map::new();
        let mut seen_node_ids = HbHashSet::new();

        for entry in metadata {
            if seen_node_ids.insert(entry.id) {
                *capacity_by_parent_tag
                    .entry(encode_parent_tag_key(entry.parent_id, entry.tag_id))
                    .or_insert(0) += 1;
                *capacity_by_tag.entry(entry.tag_id).or_insert(0) += 1;
            }
        }

        let mut ids_by_parent_tag: Map<u64, Vec<u32>> = capacity_by_parent_tag
            .into_iter()
            .map(|(key, capacity)| (key, Vec::with_capacity(capacity)))
            .collect();

        let mut ids_by_tag: Map<TagId, Vec<u32>> = capacity_by_tag
            .into_iter()
            .map(|(tag, capacity)| (tag, Vec::with_capacity(capacity)))
            .collect();

        seen_node_ids.clear();
        for entry in metadata {
            if seen_node_ids.insert(entry.id) {
                ids_by_parent_tag
                    .entry(encode_parent_tag_key(entry.parent_id, entry.tag_id))
                    .or_default()
                    .push(entry.id);
                ids_by_tag.entry(entry.tag_id).or_default().push(entry.id);
            }
        }

        Self {
            ids_by_parent_tag,
            ids_by_tag,
        }
    }

    pub(crate) fn get_param_rows_into<'a, P: MetadataPolicy>(
        &self,
        owner_rows: &'a OwnerRows<'a>,
        entity_id: u32,
        policy: &P,
        out: &mut Vec<&'a Metadatum>,
    ) {
        for &row in owner_rows.get(entity_id) {
            if policy.is_param(row.tag_id) && !policy.should_exclude(row) {
                out.push(row);
            }
        }

        for &traversal_tag in policy.traversal_tags() {
            let key = encode_parent_tag_key(entity_id, traversal_tag);
            if let Some(child_ids) = self.ids_by_parent_tag.get(&key) {
                for &child_id in child_ids {
                    if child_id == entity_id {
                        continue;
                    }
                    for &row in owner_rows.get(child_id) {
                        if policy.is_param(row.tag_id) {
                            out.push(row);
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub(crate) fn ids_for(&self, parent_id: u32, tag: TagId) -> &[u32] {
        self.ids_by_parent_tag
            .get(&encode_parent_tag_key(parent_id, tag))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    #[inline]
    pub(crate) fn all_ids(&self, tag: TagId) -> &[u32] {
        self.ids_by_tag
            .get(&tag)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

pub(crate) struct DefaultMetadataPolicy;

impl MetadataPolicy for DefaultMetadataPolicy {
    #[inline]
    fn is_param(&self, tag: TagId) -> bool {
        matches!(
            tag,
            TagId::CvParam | TagId::UserParam | TagId::ReferenceableParamGroupRef
        )
    }

    #[inline]
    fn should_exclude(&self, m: &Metadatum) -> bool {
        m.accession
            .as_deref()
            .map_or(false, |a| a.starts_with(EXCLUDED_ACCESSION_PREFIX))
    }

    #[inline]
    fn traversal_tags(&self) -> &[TagId] {
        &[
            TagId::ReferenceableParamGroupRef,
            TagId::CvParam,
            TagId::UserParam,
        ]
    }
}

#[inline]
const fn encode_parent_tag_key(parent_id: u32, tag: TagId) -> u64 {
    ((parent_id as u64) << 32) | (tag as u32 as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::decode::MetadatumValue;

    fn metadatum(id: u32, parent_id: u32, tag_id: TagId) -> Metadatum {
        Metadatum {
            item_index: 0,
            id,
            parent_id,
            tag_id,
            accession: None,
            unit_accession: None,
            value: MetadatumValue::Empty,
        }
    }

    fn cv_param_metadatum(id: u32, parent_id: u32, accession: &str) -> Metadatum {
        Metadatum {
            item_index: 0,
            id,
            parent_id,
            tag_id: TagId::CvParam,
            accession: Some(accession.to_string()),
            unit_accession: None,
            value: MetadatumValue::Empty,
        }
    }

    #[test]
    fn owner_rows_get_returns_empty_for_unknown_id() {
        let rows = OwnerRows::with_capacity(0);
        assert!(rows.get(42).is_empty());
    }

    #[test]
    fn owner_rows_accumulates_multiple_rows_under_same_id() {
        let metadata = vec![
            metadatum(1, 0, TagId::Spectrum),
            metadatum(1, 0, TagId::CvParam),
        ];
        let mut rows = OwnerRows::with_capacity(2);
        for m in &metadata {
            rows.insert(m.id, m);
        }
        assert_eq!(rows.get(1).len(), 2);
        assert!(rows.get(2).is_empty());
    }

    #[test]
    fn children_lookup_ids_for_finds_children_by_parent_and_tag() {
        let metadata = vec![
            metadatum(1, 0, TagId::SpectrumList),
            metadatum(2, 1, TagId::Spectrum),
            metadatum(3, 1, TagId::Spectrum),
            metadatum(4, 1, TagId::CvParam),
        ];
        let lookup = ChildrenLookup::new(&metadata);

        let spectra_under_list = lookup.ids_for(1, TagId::Spectrum);
        assert_eq!(spectra_under_list.len(), 2);
        assert!(spectra_under_list.contains(&2));
        assert!(spectra_under_list.contains(&3));

        let cv_params_under_list = lookup.ids_for(1, TagId::CvParam);
        assert_eq!(cv_params_under_list.len(), 1);
        assert!(cv_params_under_list.contains(&4));
    }

    #[test]
    fn children_lookup_all_ids_returns_every_node_with_given_tag() {
        let metadata = vec![
            metadatum(10, 0, TagId::Spectrum),
            metadatum(11, 0, TagId::Spectrum),
            metadatum(12, 0, TagId::CvParam),
        ];
        let lookup = ChildrenLookup::new(&metadata);

        let all_spectra = lookup.all_ids(TagId::Spectrum);
        assert_eq!(all_spectra.len(), 2);
        assert!(all_spectra.contains(&10));
        assert!(all_spectra.contains(&11));

        assert_eq!(lookup.all_ids(TagId::CvParam).len(), 1);
        assert!(lookup.all_ids(TagId::ChromatogramList).is_empty());
    }

    #[test]
    fn children_lookup_deduplicates_nodes_with_repeated_id() {
        let metadata = vec![
            metadatum(5, 0, TagId::Spectrum),
            metadatum(5, 0, TagId::CvParam),
        ];
        let lookup = ChildrenLookup::new(&metadata);
        assert_eq!(lookup.all_ids(TagId::Spectrum).len(), 1);
        assert!(lookup.all_ids(TagId::CvParam).is_empty());
    }

    #[test]
    fn children_lookup_ids_for_returns_empty_for_nonexistent_parent() {
        let metadata = vec![metadatum(1, 0, TagId::Spectrum)];
        let lookup = ChildrenLookup::new(&metadata);
        assert!(lookup.ids_for(999, TagId::Spectrum).is_empty());
    }

    #[test]
    fn get_param_rows_into_collects_direct_cv_params_for_entity() {
        let spectrum = metadatum(1, 0, TagId::Spectrum);
        let param = cv_param_metadatum(2, 1, "MS:1000511");
        let metadata = vec![spectrum, param.clone()];

        let mut owner_rows = OwnerRows::with_capacity(2);
        for m in &metadata {
            owner_rows.insert(m.id, m);
        }

        let lookup = ChildrenLookup::new(&metadata);
        let policy = DefaultMetadataPolicy;
        let mut collected = Vec::new();
        lookup.get_param_rows_into(&owner_rows, 2, &policy, &mut collected);

        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].accession.as_deref(), Some("MS:1000511"));
    }

    #[test]
    fn default_policy_excludes_b000_prefixed_accessions() {
        let row = Metadatum {
            item_index: 0,
            id: 1,
            parent_id: 0,
            tag_id: TagId::CvParam,
            accession: Some("B000:9910001".to_string()),
            unit_accession: None,
            value: MetadatumValue::Empty,
        };
        assert!(DefaultMetadataPolicy.should_exclude(&row));
    }

    #[test]
    fn default_policy_does_not_exclude_ms_accessions() {
        let row = Metadatum {
            item_index: 0,
            id: 1,
            parent_id: 0,
            tag_id: TagId::CvParam,
            accession: Some("MS:1000511".to_string()),
            unit_accession: None,
            value: MetadatumValue::Empty,
        };
        assert!(!DefaultMetadataPolicy.should_exclude(&row));
    }

    #[test]
    fn encode_parent_tag_key_is_unique_across_different_parent_ids_and_tags() {
        let key_parent_1_spectrum = encode_parent_tag_key(1, TagId::Spectrum);
        let key_parent_2_spectrum = encode_parent_tag_key(2, TagId::Spectrum);
        let key_parent_1_cv = encode_parent_tag_key(1, TagId::CvParam);

        assert_ne!(key_parent_1_spectrum, key_parent_2_spectrum);
        assert_ne!(key_parent_1_spectrum, key_parent_1_cv);
        assert_ne!(key_parent_2_spectrum, key_parent_1_cv);
    }

    #[test]
    fn encode_parent_tag_key_is_stable_for_same_inputs() {
        let first = encode_parent_tag_key(7, TagId::Chromatogram);
        let second = encode_parent_tag_key(7, TagId::Chromatogram);
        assert_eq!(first, second);
    }
}
