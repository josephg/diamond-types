/// This is a tiny library to convert from codepoint offsets in a utf-8 string to byte offsets, and
/// back.
///
/// Its super weird that rust doesn't have anything like this in the standard library (as far as I
/// can tell). You can fake it with char_indices().nth()... but the resulting generated code is
/// *awful*.

fn codepoint_size(b: u8) -> usize {
    match b {
        0 => usize::MAX, // null byte. Usually invalid here.
        0b0000_0001..=0b0111_1111 => 1,
        0b1000_0000..=0b1011_1111 => usize::MAX, // Invalid for a starting byte
        0b1100_0000..=0b1101_1111 => 2,
        0b1110_0000..=0b1110_1111 => 3,
        0b1111_0000..=0b1111_0111 => 4,
        0b1111_1000..=0b1111_1011 => 5,
        0b1111_1100..=0b1111_1101 => 6,
        _ => usize::MAX,
    }
}

// I'm sure there's much better ways to write this. But this is fine for now - its not a bottleneck.
// Code adapted from here:
// https://github.com/josephg/librope/blob/785a7c5ef6dc6ca05cb545264fbb22c96951af0d/rope.c#L193-L212
pub fn chars_to_bytes_smol(s: &str, char_pos: usize) -> usize {
    let bytes = s.as_bytes();
    let mut num_bytes = 0;

    for _i in 0..char_pos {
        assert!(num_bytes < bytes.len());
        num_bytes += codepoint_size(bytes[num_bytes]);
    }
    num_bytes
}

pub fn chars_to_bytes(s: &str, char_pos: usize) -> usize {
    // For all that my implementation above is correct and tight, ropey's char_to_byte_idx is
    // already being pulled in anyway by ropey, and its faster. Just use that.
    ropey::str_utils::char_to_byte_idx(s, char_pos)
}

pub fn split_at_char(s: &str, char_pos: usize) -> (&str, &str) {
    s.split_at(chars_to_bytes(s, char_pos))
}

pub fn bytes_to_chars(s: &str, byte_pos: usize) -> usize {
    ropey::str_utils::byte_to_char_idx(s, byte_pos)
}

pub fn count_chars(s: &str) -> usize {
    ropey::str_utils::byte_to_char_idx(s, s.len())
}

#[cfg(test)]
mod test {
    use crate::unicount::*;

    // TODO: Run a microbenchmark to see how this performs in the wild.
    fn std_chars_to_bytes(s: &str, char_pos: usize) -> usize {
        s.char_indices().nth(char_pos).map_or_else(
            || s.len(),
            |(i, _)| i
        )
    }

    pub fn std_bytes_to_chars(s: &str, byte_pos: usize) -> usize {
        s[..byte_pos].chars().count()
    }

    const TRICKY_CHARS: &[&str] = &[
        "a", "b", "c", "1", "2", "3", " ", "\n", // ASCII
        "©", "¥", "½", // The Latin-1 suppliment (U+80 - U+ff)
        "Ύ", "Δ", "δ", "Ϡ", // Greek (U+0370 - U+03FF)
        "←", "↯", "↻", "⇈", // Arrows (U+2190 – U+21FF)
        "𐆐", "𐆔", "𐆘", "𐆚", // Ancient roman symbols (U+10190 – U+101CF)
    ];

    fn check_matches(s: &str) {
        let char_len = s.chars().count();
        for i in 0..=char_len {
            let expected_bytes = std_chars_to_bytes(s, i);
            let actual_bytes = chars_to_bytes_smol(s, i);
            let ropey_bytes = ropey::str_utils::char_to_byte_idx(s, i);
            // dbg!(expected, actual);
            assert_eq!(expected_bytes, actual_bytes);
            assert_eq!(ropey_bytes, actual_bytes);

            let std_chars = std_bytes_to_chars(s, actual_bytes);
            let ropey_chars = bytes_to_chars(s, actual_bytes);

            assert_eq!(std_chars, i);
            assert_eq!(ropey_chars, i);
        }
    }

    #[test]
    fn str_pos_works() {
        check_matches("hi");
        check_matches("");
        for s in TRICKY_CHARS {
            check_matches(*s);
        }

        // And throw them all in a big string.
        let mut big_str = String::new();
        for s in TRICKY_CHARS {
            big_str.push_str(*s);
        }
        check_matches(big_str.as_str());
    }

    #[test]
    fn test_split_at_char() {
        assert_eq!(split_at_char("", 0), ("", ""));
        assert_eq!(split_at_char("hi", 0), ("", "hi"));
        assert_eq!(split_at_char("hi", 1), ("h", "i"));
        assert_eq!(split_at_char("hi", 2), ("hi", ""));

        assert_eq!(split_at_char("日本語", 0), ("", "日本語"));
        assert_eq!(split_at_char("日本語", 1), ("日", "本語"));
        assert_eq!(split_at_char("日本語", 2), ("日本", "語"));
        assert_eq!(split_at_char("日本語", 3), ("日本語", ""));
    }
}

