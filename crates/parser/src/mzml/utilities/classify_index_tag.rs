#[derive(PartialEq)]
pub enum IndexTag {
    IndexList,
    Index,
    Offset,
    IndexListOffset,
    FileChecksum,
    Other,
}

#[inline]
pub fn classify_index_tag(raw: &[u8]) -> IndexTag {
    match raw {
        b"indexList" => IndexTag::IndexList,
        b"index" => IndexTag::Index,
        b"offset" => IndexTag::Offset,
        b"indexListOffset" => IndexTag::IndexListOffset,
        b"fileChecksum" => IndexTag::FileChecksum,
        _ => IndexTag::Other,
    }
}
