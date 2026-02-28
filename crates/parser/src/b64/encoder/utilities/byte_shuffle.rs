#[inline(always)]
pub(crate) fn shuffle_bytes_by_stride(input: &[u8], output: &mut [u8], element_stride: usize) {
    match element_stride {
        8 => shuffle_eight_byte_elements(input, output),
        4 => shuffle_four_byte_elements(input, output),
        2 => shuffle_two_byte_elements(input, output),
        _ => shuffle_arbitrary_stride(input, output, element_stride),
    }
}

#[inline(always)]
fn shuffle_two_byte_elements(input: &[u8], output: &mut [u8]) {
    let half_len = input.len() / 2;
    let (low_bytes, high_bytes) = output.split_at_mut(half_len);
    for element_index in 0..half_len {
        low_bytes[element_index] = input[element_index * 2];
        high_bytes[element_index] = input[element_index * 2 + 1];
    }
}

#[inline(always)]
fn shuffle_four_byte_elements(input: &[u8], output: &mut [u8]) {
    let element_count = input.len() / 4;
    let (byte0_lane, remainder) = output.split_at_mut(element_count);
    let (byte1_lane, remainder) = remainder.split_at_mut(element_count);
    let (byte2_lane, byte3_lane) = remainder.split_at_mut(element_count);
    for element_index in 0..element_count {
        let input_offset = element_index * 4;
        byte0_lane[element_index] = input[input_offset];
        byte1_lane[element_index] = input[input_offset + 1];
        byte2_lane[element_index] = input[input_offset + 2];
        byte3_lane[element_index] = input[input_offset + 3];
    }
}

#[inline(always)]
fn shuffle_eight_byte_elements(input: &[u8], output: &mut [u8]) {
    let element_count = input.len() / 8;
    let (byte0_lane, remainder) = output.split_at_mut(element_count);
    let (byte1_lane, remainder) = remainder.split_at_mut(element_count);
    let (byte2_lane, remainder) = remainder.split_at_mut(element_count);
    let (byte3_lane, remainder) = remainder.split_at_mut(element_count);
    let (byte4_lane, remainder) = remainder.split_at_mut(element_count);
    let (byte5_lane, remainder) = remainder.split_at_mut(element_count);
    let (byte6_lane, byte7_lane) = remainder.split_at_mut(element_count);
    for element_index in 0..element_count {
        let input_offset = element_index * 8;
        byte0_lane[element_index] = input[input_offset];
        byte1_lane[element_index] = input[input_offset + 1];
        byte2_lane[element_index] = input[input_offset + 2];
        byte3_lane[element_index] = input[input_offset + 3];
        byte4_lane[element_index] = input[input_offset + 4];
        byte5_lane[element_index] = input[input_offset + 5];
        byte6_lane[element_index] = input[input_offset + 6];
        byte7_lane[element_index] = input[input_offset + 7];
    }
}

#[inline(always)]
fn shuffle_arbitrary_stride(input: &[u8], output: &mut [u8], element_stride: usize) {
    let element_count = input.len() / element_stride;
    for byte_position in 0..element_stride {
        let lane_start = byte_position * element_count;
        for element_index in 0..element_count {
            output[lane_start + element_index] =
                input[byte_position + element_index * element_stride];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shuffle_two_byte_elements_correctness() {
        let input = [1u8, 2, 3, 4];
        let mut output = [0u8; 4];
        shuffle_two_byte_elements(&input, &mut output);
        assert_eq!(output, [1, 3, 2, 4]);
    }

    #[test]
    fn shuffle_four_byte_elements_correctness() {
        let input: Vec<u8> = (0u8..8).collect();
        let mut output = vec![0u8; 8];
        shuffle_four_byte_elements(&input, &mut output);
        assert_eq!(output, [0, 4, 1, 5, 2, 6, 3, 7]);
    }

    #[test]
    fn shuffle_eight_byte_elements_correctness() {
        let input: Vec<u8> = (0u8..16).collect();
        let mut output = vec![0u8; 16];
        shuffle_eight_byte_elements(&input, &mut output);
        assert_eq!(
            output,
            [0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15]
        );
    }

    #[test]
    fn shuffle_arbitrary_stride_matches_four_byte_specialized() {
        let input: Vec<u8> = (0u8..8).collect();
        let mut specific_output = vec![0u8; 8];
        let mut generic_output = vec![0u8; 8];
        shuffle_four_byte_elements(&input, &mut specific_output);
        shuffle_arbitrary_stride(&input, &mut generic_output, 4);
        assert_eq!(specific_output, generic_output);
    }

    #[test]
    fn shuffle_dispatch_routes_correctly() {
        let input: Vec<u8> = (0u8..8).collect();
        let mut via_dispatch = vec![0u8; 8];
        let mut via_direct = vec![0u8; 8];
        shuffle_bytes_by_stride(&input, &mut via_dispatch, 4);
        shuffle_four_byte_elements(&input, &mut via_direct);
        assert_eq!(via_dispatch, via_direct);
    }

    #[test]
    fn shuffle_one_byte_stride_is_identity() {
        let input = [10u8, 20, 30, 40];
        let mut output = [0u8; 4];
        shuffle_bytes_by_stride(&input, &mut output, 1);
        assert_eq!(output, input);
    }
}
