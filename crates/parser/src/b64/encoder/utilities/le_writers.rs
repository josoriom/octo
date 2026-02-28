use std::slice;

#[inline]
pub(crate) fn write_u32_le(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub(crate) fn write_u64_le(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub(crate) fn write_f32_le(buf: &mut Vec<u8>, v: f32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub(crate) fn write_f64_le(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u16_le_scalar(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_i16_le_scalar(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_i32_le_scalar(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_i64_le_scalar(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

macro_rules! write_typed_slice_le {
    ($fn_name:ident, $elem_ty:ty, $elem_size:expr, $scalar_writer:ident) => {
        pub(crate) fn $fn_name(buf: &mut Vec<u8>, values: &[$elem_ty]) {
            if cfg!(target_endian = "little") {
                unsafe {
                    buf.extend_from_slice(slice::from_raw_parts(
                        values.as_ptr() as *const u8,
                        values.len() * $elem_size,
                    ));
                }
            } else {
                for &v in values {
                    $scalar_writer(buf, v);
                }
            }
        }
    };
}

write_typed_slice_le!(write_u16_slice_le, u16, 2, write_u16_le_scalar);
write_typed_slice_le!(write_i16_slice_le, i16, 2, write_i16_le_scalar);
write_typed_slice_le!(write_i32_slice_le, i32, 4, write_i32_le_scalar);
write_typed_slice_le!(write_i64_slice_le, i64, 8, write_i64_le_scalar);
write_typed_slice_le!(write_u32_slice_le, u32, 4, write_u32_le);
write_typed_slice_le!(write_f32_slice_le, f32, 4, write_f32_le);
write_typed_slice_le!(write_f64_slice_le, f64, 8, write_f64_le);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_u32_le_produces_little_endian() {
        let mut buf = Vec::new();
        write_u32_le(&mut buf, 0x04030201);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn write_u64_le_produces_little_endian() {
        let mut buf = Vec::new();
        write_u64_le(&mut buf, 0x0807060504030201);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn write_f32_le_roundtrips() {
        let mut buf = Vec::new();
        write_f32_le(&mut buf, 1.0f32);
        assert_eq!(buf, 1.0f32.to_le_bytes());
    }

    #[test]
    fn write_f64_le_roundtrips() {
        let mut buf = Vec::new();
        write_f64_le(&mut buf, 1.0f64);
        assert_eq!(buf, 1.0f64.to_le_bytes());
    }

    #[test]
    fn write_u32_slice_le_matches_scalar() {
        let values = [1u32, 2, 3];
        let mut via_slice = Vec::new();
        let mut via_scalar = Vec::new();
        write_u32_slice_le(&mut via_slice, &values);
        for &v in &values {
            write_u32_le(&mut via_scalar, v);
        }
        assert_eq!(via_slice, via_scalar);
    }

    #[test]
    fn write_f64_slice_le_matches_scalar() {
        let values = [1.5f64, 2.5, 3.5];
        let mut via_slice = Vec::new();
        let mut via_scalar = Vec::new();
        write_f64_slice_le(&mut via_slice, &values);
        for &v in &values {
            write_f64_le(&mut via_scalar, v);
        }
        assert_eq!(via_slice, via_scalar);
    }

    #[test]
    fn write_i16_slice_le_matches_scalar() {
        let values = [-1i16, 0, 1, 32767];
        let mut via_slice = Vec::new();
        let mut via_scalar = Vec::new();
        write_i16_slice_le(&mut via_slice, &values);
        for &v in &values {
            write_i16_le_scalar(&mut via_scalar, v);
        }
        assert_eq!(via_slice, via_scalar);
    }

    #[test]
    fn write_empty_slice_is_noop() {
        let mut buf = Vec::new();
        write_u32_slice_le(&mut buf, &[]);
        assert!(buf.is_empty());
    }

    #[test]
    fn write_u16_slice_le_correct_byte_count() {
        let mut buf = Vec::new();
        write_u16_slice_le(&mut buf, &[1u16, 2, 3]);
        assert_eq!(buf.len(), 6);
    }

    #[test]
    fn write_i64_slice_le_correct_byte_count() {
        let mut buf = Vec::new();
        write_i64_slice_le(&mut buf, &[1i64, 2]);
        assert_eq!(buf.len(), 16);
    }
}
