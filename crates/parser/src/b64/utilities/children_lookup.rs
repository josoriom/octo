use std::{
    borrow::Borrow,
    collections::{HashMap as StdHashMap, HashSet as StdHashSet},
    ops::{Deref, DerefMut},
};

use crate::{decode::Metadatum, mzml::schema::TagId};
use rustc_hash::FxBuildHasher;

type BuildHasher = FxBuildHasher;
// type BuildHasher = std::collections::hash_map::RandomState;

type Map<K, V> = StdHashMap<K, V, BuildHasher>;
type Set<T> = StdHashSet<T, BuildHasher>;

#[inline]
fn map_with_capacity<K, V>(cap: usize) -> Map<K, V> {
    Map::with_capacity_and_hasher(cap, Default::default())
}

#[inline]
fn map_new<K, V>() -> Map<K, V> {
    Map::with_hasher(Default::default())
}

#[inline]
fn set_with_capacity<T>(cap: usize) -> Set<T> {
    Set::with_capacity_and_hasher(cap, Default::default())
}

#[inline]
pub fn key_parent_tag(parent_id: u32, tag: TagId) -> u64 {
    ((parent_id as u64) << 8) | (tag as u8 as u64)
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
}

impl<'m> Default for OwnerRows<'m> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<'m> Deref for OwnerRows<'m> {
    type Target = Map<u32, Vec<&'m Metadatum>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'m> DerefMut for OwnerRows<'m> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
}

impl ChildrenLookup {
    #[inline]
    pub fn new<M: Borrow<Metadatum>>(metadata: &[M]) -> Self {
        let len = metadata.len();

        let mut count_by_parent_tag: Map<u64, usize> = map_with_capacity(len);
        let mut count_by_parent: Map<u32, usize> = map_with_capacity(len);

        for m in metadata {
            let m = m.borrow();
            *count_by_parent_tag
                .entry(key_parent_tag(m.parent_index, m.tag_id))
                .or_insert(0) += 1;
            *count_by_parent.entry(m.parent_index).or_insert(0) += 1;
        }

        let mut ids_by_parent_tag: Map<u64, Vec<u32>> =
            map_with_capacity(count_by_parent_tag.len());
        for (k, c) in count_by_parent_tag {
            ids_by_parent_tag.insert(k, Vec::with_capacity(c));
        }

        let mut ids_by_parent: Map<u32, Vec<u32>> = map_with_capacity(count_by_parent.len());
        for (k, c) in count_by_parent {
            ids_by_parent.insert(k, Vec::with_capacity(c));
        }

        for m in metadata {
            let m = m.borrow();

            let k = key_parent_tag(m.parent_index, m.tag_id);
            let v = ids_by_parent_tag.get_mut(&k).unwrap();
            if v.last().copied() != Some(m.id) {
                v.push(m.id);
            }

            let v2 = ids_by_parent.get_mut(&m.parent_index).unwrap();
            if v2.last().copied() != Some(m.id) {
                v2.push(m.id);
            }
        }

        Self {
            ids_by_parent_tag,
            ids_by_parent,
        }
    }

    #[inline]
    pub fn owner_rows_with_capacity<'m>(len: usize) -> OwnerRows<'m> {
        OwnerRows::with_capacity(len)
    }

    // FIX: separate borrow lifetime ('a) from the stored-ref lifetime ('m)
    #[inline]
    pub fn rows_for_owner<'a, 'm>(owner_rows: &'a OwnerRows<'m>, id: u32) -> &'a [&'m Metadatum] {
        owner_rows.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    #[inline]
    pub fn get_children_with_tag(&self, parent_id: u32, tag: TagId) -> &[u32] {
        self.ids_by_parent_tag
            .get(&key_parent_tag(parent_id, tag))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    #[inline]
    pub fn ids_for(&self, metadata: &[&Metadatum], parent_id: u32, tag: TagId) -> Vec<u32> {
        let direct = self.get_children_with_tag(parent_id, tag);

        if direct.len() == 1 {
            return vec![direct[0]];
        }
        if !direct.is_empty() {
            return direct.to_vec();
        }

        let mut out = Vec::new();
        let mut seen = set_with_capacity(metadata.len().min(1024));
        for &m in metadata {
            if m.tag_id == tag && m.parent_index == parent_id && seen.insert(m.id) {
                out.push(m.id);
            }
        }
        out
    }

    #[inline]
    pub fn ids_for_tags(
        &self,
        metadata: &[&Metadatum],
        parent_id: u32,
        tags: &[TagId],
    ) -> Vec<u32> {
        let mut direct_count = 0usize;
        for &t in tags {
            direct_count += self.get_children_with_tag(parent_id, t).len();
        }

        if direct_count > 0 {
            let mut out = Vec::with_capacity(direct_count);
            for &t in tags {
                out.extend_from_slice(self.get_children_with_tag(parent_id, t));
            }
            return out;
        }

        let mut out = Vec::new();
        let mut seen = set_with_capacity(metadata.len().min(1024));

        for &m in metadata {
            if m.parent_index != parent_id {
                continue;
            }
            let mut ok = false;
            for &t in tags {
                if m.tag_id == t {
                    ok = true;
                    break;
                }
            }
            if ok && seen.insert(m.id) {
                out.push(m.id);
            }
        }

        out
    }

    #[inline]
    pub fn param_rows<'a>(
        &self,
        metadata: &[&'a Metadatum],
        owner_rows: &OwnerRows<'a>,
        parent_id: u32,
    ) -> Vec<&'a Metadatum> {
        let cv_ids = self.ids_for(metadata, parent_id, TagId::CvParam);
        let up_ids = self.ids_for(metadata, parent_id, TagId::UserParam);

        let mut out = Vec::with_capacity(cv_ids.len() + up_ids.len());

        for id in cv_ids {
            if let Some(rows) = owner_rows.get(&id) {
                out.extend(rows.iter().copied());
            }
        }
        for id in up_ids {
            if let Some(rows) = owner_rows.get(&id) {
                out.extend(rows.iter().copied());
            }
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
    pub fn all_ids(metadata: &[&Metadatum], tag: TagId) -> Vec<u32> {
        let mut out = Vec::new();
        let mut seen = set_with_capacity(metadata.len().min(1024));

        for m in metadata {
            if m.tag_id == tag && seen.insert(m.id) {
                out.push(m.id);
            }
        }

        out
    }
}
