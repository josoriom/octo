pub mod mzml;
pub use mzml::{bin_to_mzml, parse_indexed_mzml, parse_mzml, structs::*};
pub mod b64;
pub use b64::{decoder, encoder, utilities::Header};
pub mod utilities;
