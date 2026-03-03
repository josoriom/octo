pub mod decoder;
pub use decoder::decode::decode;
pub(crate) use decoder::utilities;
pub mod encoder;
pub use encoder::{encode::WritingMode, encode::encode, utilities::FileEncoderOutput};
pub mod attr_meta;
