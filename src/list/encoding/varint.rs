/// We're using protobuf's encoding system for variable sized integers. Most numbers we store here
/// follow a Parato distribution, so this ends up being a space savings overall.
///
/// The encoding format is described in much more detail
/// [in google's protobuf documentation](https://developers.google.com/protocol-buffers/docs/encoding)
///
/// This code has been stolen with love from [rust-protobuf](https://github.com/stepancheg/rust-protobuf/blob/681462cc2a7068a2ff4111bbf19861c005c38225/protobuf/src/varint.rs)
///
/// (With some modifications.)

/// Encode u64 as varint.
/// Panics if buffer length is less than 10.
pub fn encode_u64(mut value: u64, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= 10);

    fn iter(value: &mut u64, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    if !iter(&mut value, &mut buf[4]) {
        return 5;
    };
    if !iter(&mut value, &mut buf[5]) {
        return 6;
    };
    if !iter(&mut value, &mut buf[6]) {
        return 7;
    };
    if !iter(&mut value, &mut buf[7]) {
        return 8;
    };
    if !iter(&mut value, &mut buf[8]) {
        return 9;
    };
    buf[9] = value as u8;
    10
}

/// Encode u32 value as varint.
/// Panics if buffer length is less than 5.
pub fn encode_u32(mut value: u32, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= 5);

    fn iter(value: &mut u32, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    buf[4] = value as u8;
    5
}

// Who coded it better?
// pub fn encode_zig_zag_32(n: i32) -> u32 {
//     ((n << 1) ^ (n >> 31)) as u32
// }
//
// pub fn encode_zig_zag_64(n: i64) -> u64 {
//     ((n << 1) ^ (n >> 63)) as u64
// }

fn encode_zigzag_i64(val: i64) -> u64 {
    val.abs() as u64 * 2 + val.is_negative() as u64
}

fn encode_zigzag_i32(val: i32) -> u32 {
    val.abs() as u32 * 2 + val.is_negative() as u32
}

pub fn encode_i64(value: i64, buf: &mut[u8]) -> usize {
    encode_u64(encode_zigzag_i64(value), buf)
}

pub fn encode_i32(value: i32, buf: &mut[u8]) -> usize {
    encode_u32(encode_zigzag_i32(value), buf)
}

#[cfg(test)]
mod test {
    use super::encode_u32;

    #[test]
    fn simple_encode_u32() {
        // This isn't thorough, but its a decent smoke test.
        // Encoding example from https://developers.google.com/protocol-buffers/docs/encoding:
        let mut result = [0u8; 5];
        assert_eq!(2, encode_u32(300, &mut result[..]));
        assert_eq!(result[0], 0b10101100);
        assert_eq!(result[1], 0b00000010);
    }
}