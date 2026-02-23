use crate::{decoder::decode::Metadatum, mzml::schema::TagId};
use hashbrown::{HashMap as HbHashMap, HashSet as HbHashSet};

type Map<K, V> = HbHashMap<K, V>;
type Set<T> = HbHashSet<T>;

const EXCLUDED_ACCESSION_PREFIX: &str = "B000:";

pub trait MetadataPolicy {
    fn is_param(&self, tag: TagId) -> bool;
    fn should_exclude(&self, m: &Metadatum) -> bool;
    fn traversal_tags(&self) -> &[TagId];
}

#[repr(transparent)]
pub struct OwnerRows<'m>(Map<u32, Vec<&'m Metadatum>>);

impl<'m> OwnerRows<'m> {
    #[inline]
    pub fn new() -> Self {
        Self(Map::new())
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self(Map::with_capacity(cap))
    }

    #[inline]
    pub fn get(&self, id: u32) -> &[&'m Metadatum] {
        self.0.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    #[inline]
    pub fn insert(&mut self, id: u32, row: &'m Metadatum) {
        self.0.entry(id).or_default().push(row);
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'m> IntoIterator for OwnerRows<'m> {
    type Item = (u32, Vec<&'m Metadatum>);
    type IntoIter = hashbrown::hash_map::IntoIter<u32, Vec<&'m Metadatum>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct ChildrenLookup {
    ids_by_parent_tag: Map<u64, Vec<u32>>,
    ids_by_parent: Map<u32, Vec<u32>>,
    ids_by_tag: Map<TagId, Vec<u32>>,
}

impl ChildrenLookup {
    pub fn new(metadata: &[Metadatum]) -> Self {
        let mut count_by_parent_tag: Map<u64, usize> = Map::new();
        let mut count_by_parent: Map<u32, usize> = Map::new();
        let mut count_by_tag: Map<TagId, usize> = Map::new();
        let mut visited = HbHashSet::new();

        for entry in metadata {
            if visited.insert(entry.id) {
                let parent_tag_key = key_parent_tag(entry.parent_id, entry.tag_id);
                *count_by_parent_tag.entry(parent_tag_key).or_insert(0) += 1;
                *count_by_parent.entry(entry.parent_id).or_insert(0) += 1;
                *count_by_tag.entry(entry.tag_id).or_insert(0) += 1;
            }
        }

        let mut ids_by_parent_tag = Map::with_capacity(count_by_parent_tag.len());
        for (key, capacity) in count_by_parent_tag {
            ids_by_parent_tag.insert(key, Vec::with_capacity(capacity));
        }

        let mut ids_by_parent = Map::with_capacity(count_by_parent.len());
        for (parent_id, capacity) in count_by_parent {
            ids_by_parent.insert(parent_id, Vec::with_capacity(capacity));
        }

        let mut ids_by_tag = Map::with_capacity(count_by_tag.len());
        for (tag, capacity) in count_by_tag {
            ids_by_tag.insert(tag, Vec::with_capacity(capacity));
        }

        visited.clear();
        for entry in metadata {
            if visited.insert(entry.id) {
                let parent_tag_key = key_parent_tag(entry.parent_id, entry.tag_id);
                ids_by_parent_tag
                    .get_mut(&parent_tag_key)
                    .unwrap()
                    .push(entry.id);
                ids_by_parent
                    .get_mut(&entry.parent_id)
                    .unwrap()
                    .push(entry.id);
                ids_by_tag.get_mut(&entry.tag_id).unwrap().push(entry.id);
            }
        }

        Self {
            ids_by_parent_tag,
            ids_by_parent,
            ids_by_tag,
        }
    }

    pub fn get_param_rows_into<'a, P: MetadataPolicy>(
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

        for &tag in policy.traversal_tags() {
            let parent_tag_key = key_parent_tag(entity_id, tag);
            if let Some(child_ids) = self.ids_by_parent_tag.get(&parent_tag_key) {
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
    pub fn ids_for(&self, parent_id: u32, tag: TagId) -> &[u32] {
        self.ids_by_parent_tag
            .get(&key_parent_tag(parent_id, tag))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    #[inline]
    pub fn all_ids(&self, tag: TagId) -> &[u32] {
        self.ids_by_tag
            .get(&tag)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    #[inline]
    pub fn subtree_ids_into(&self, root_id: u32, visited: &mut Set<u32>) {
        let mut stack = Vec::with_capacity(32);
        stack.push(root_id);
        while let Some(current_id) = stack.pop() {
            if visited.insert(current_id) {
                if let Some(children) = self.ids_by_parent.get(&current_id) {
                    stack.extend_from_slice(children);
                }
            }
        }
    }
}

pub struct DefaultMetadataPolicy;

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
const fn key_parent_tag(parent_id: u32, tag: TagId) -> u64 {
    ((parent_id as u64) << 32) | (tag as u32 as u64)
}
