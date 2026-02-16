use std::borrow::Borrow;

use crate::{decode::Metadatum, mzml::schema::TagId};
use hashbrown::{HashMap as HbHashMap, HashSet as HbHashSet};

type Map<K, V> = HbHashMap<K, V>;
type Set<T> = HbHashSet<T>;

#[inline]
fn map_with_capacity<K, V>(cap: usize) -> Map<K, V> {
    Map::with_capacity(cap)
}

#[inline]
fn map_new<K, V>() -> Map<K, V> {
    Map::new()
}

#[inline]
fn set_with_capacity<T>(cap: usize) -> Set<T> {
    Set::with_capacity(cap)
}

#[inline]
pub fn key_parent_tag(parent_id: u32, tag: TagId) -> u64 {
    ((parent_id as u64) << 32) | (tag as u32 as u64)
}

#[repr(transparent)]
pub struct OwnerRows<'m>(Map<u32, Vec<&'m Metadatum>>);

impl<'m> OwnerRows<'m> {
    #[inline]
    pub fn new() -> Self {
        Self(map_new())
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self(map_with_capacity(cap))
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

impl<'m> Default for OwnerRows<'m> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<'m> IntoIterator for OwnerRows<'m> {
    type Item = (u32, Vec<&'m Metadatum>);
    type IntoIter = <Map<u32, Vec<&'m Metadatum>> as IntoIterator>::IntoIter;

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
    pub fn new<M: Borrow<Metadatum>>(metadata: &[M]) -> Self {
        let len = metadata.len();

        let mut count_by_parent_tag: Map<u64, usize> = Map::with_capacity(len);
        let mut count_by_parent: Map<u32, usize> = Map::with_capacity(len);
        let mut count_by_tag: Map<TagId, usize> = Map::with_capacity(len);

        for m in metadata {
            let m = m.borrow();
            let k = key_parent_tag(m.parent_id, m.tag_id);
            *count_by_parent_tag.entry(k).or_insert(0) += 1;
            *count_by_parent.entry(m.parent_id).or_insert(0) += 1;
            *count_by_tag.entry(m.tag_id).or_insert(0) += 1;
        }

        let mut ids_by_parent_tag = Map::with_capacity(count_by_parent_tag.len());
        let mut ids_by_parent = Map::with_capacity(count_by_parent.len());
        let mut ids_by_tag = Map::with_capacity(count_by_tag.len());

        for (k, c) in count_by_parent_tag {
            ids_by_parent_tag.insert(k, Vec::with_capacity(c));
        }
        for (k, c) in count_by_parent {
            ids_by_parent.insert(k, Vec::with_capacity(c));
        }
        for (k, c) in count_by_tag {
            ids_by_tag.insert(k, Vec::with_capacity(c));
        }

        for m in metadata {
            let m = m.borrow();
            let k = key_parent_tag(m.parent_id, m.tag_id);

            let v_pt = ids_by_parent_tag.get_mut(&k).unwrap();
            if v_pt.last().copied() != Some(m.id) {
                v_pt.push(m.id);
            }

            let v_p = ids_by_parent.get_mut(&m.parent_id).unwrap();
            if v_p.last().copied() != Some(m.id) {
                v_p.push(m.id);
            }

            let v_t = ids_by_tag.get_mut(&m.tag_id).unwrap();
            if v_t.last().copied() != Some(m.id) {
                v_t.push(m.id);
            }
        }

        Self {
            ids_by_parent_tag,
            ids_by_parent,
            ids_by_tag,
        }
    }

    #[inline]
    pub fn get_children_with_tag(&self, parent_id: u32, tag: TagId) -> &[u32] {
        self.ids_by_parent_tag
            .get(&key_parent_tag(parent_id, tag))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
    #[inline]
    pub fn ids_for(&self, parent_id: u32, tag: TagId) -> Vec<u32> {
        self.get_children_with_tag(parent_id, tag).to_vec()
    }

    #[inline]
    pub fn param_rows<'a>(&self, owner_rows: &OwnerRows<'a>, parent_id: u32) -> Vec<&'a Metadatum> {
        let cv_ids = self.ids_for(parent_id, TagId::CvParam);
        let up_ids = self.ids_for(parent_id, TagId::UserParam);

        let mut out = Vec::with_capacity(cv_ids.len() + up_ids.len());

        for id in cv_ids {
            out.extend(owner_rows.get(id).iter().copied());
        }
        for id in up_ids {
            out.extend(owner_rows.get(id).iter().copied());
        }

        out
    }

    #[inline]
    pub fn first_id(&self, parent_id: u32, tag: TagId) -> Option<u32> {
        self.get_children_with_tag(parent_id, tag).first().copied()
    }

    #[inline]
    pub fn children(&self, parent_id: u32) -> &[u32] {
        self.ids_by_parent
            .get(&parent_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    #[inline]
    pub fn subtree_ids(&self, root_id: u32) -> Set<u32> {
        let mut seen: Set<u32> = set_with_capacity(32);
        let mut stack: Vec<u32> = Vec::with_capacity(32);
        stack.push(root_id);

        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            for &ch in self.children(id) {
                stack.push(ch);
            }
        }

        seen
    }

    #[inline]
    pub fn all_ids(&self, tag: TagId) -> &[u32] {
        self.ids_by_tag
            .get(&tag)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    #[inline]
    pub fn get_param_rows<'a>(
        &self,
        owner_rows: &'a OwnerRows,
        entity_id: u32,
    ) -> Vec<&'a Metadatum> {
        let mut params = Vec::new();

        for &row in owner_rows.get(entity_id) {
            if matches!(row.tag_id, TagId::CvParam | TagId::UserParam) {
                if !row
                    .accession
                    .as_deref()
                    .map_or(false, |a| a.starts_with("B000:"))
                {
                    params.push(row);
                }
            }
        }

        for tag in [
            TagId::ReferenceableParamGroupRef,
            TagId::CvParam,
            TagId::UserParam,
        ] {
            for &child_id in self.get_children_with_tag(entity_id, tag) {
                if child_id == entity_id {
                    continue;
                }
                for &row in owner_rows.get(child_id) {
                    if matches!(row.tag_id, TagId::CvParam | TagId::UserParam) {
                        params.push(row);
                    }
                }
            }
        }
        params
    }
}
