use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum ParseError {
    Xml(quick_xml::Error),
    Base64(base64::DecodeError),
    Decompress(String),
    UnexpectedEof { context: String, byte_offset: u64 },
    UnexpectedTag { tag: String, byte_offset: u64 },
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Xml(e) => write!(f, "XML error: {e}"),
            Self::Base64(e) => write!(f, "base64 decode error: {e}"),
            Self::Decompress(s) => write!(f, "decompression error: {s}"),
            Self::UnexpectedEof {
                context,
                byte_offset,
            } => write!(
                f,
                "unexpected end of file inside <{context}> (byte {byte_offset})"
            ),
            Self::UnexpectedTag { tag, byte_offset } => {
                write!(f, "unexpected tag <{tag}> at byte {byte_offset}")
            }
        }
    }
}

impl From<quick_xml::Error> for ParseError {
    fn from(e: quick_xml::Error) -> Self {
        Self::Xml(e)
    }
}
impl From<base64::DecodeError> for ParseError {
    fn from(e: base64::DecodeError) -> Self {
        Self::Base64(e)
    }
}
