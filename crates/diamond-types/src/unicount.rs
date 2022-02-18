/// This is a tiny library to convert from codepoint offsets in a utf-8 string to byte offsets, and
/// back.
///
/// Its super weird that rust doesn't have anything like this in the standard library (as far as I
/// can tell). You can fake it with char_indices().nth()... but the resulting generated code is
/// *awful*.

pub fn chars_to_bytes(s: &str, char_pos: usize) -> usize {
    // For all that my implementation above is correct and tight, ropey's char_to_byte_idx is
    // already being pulled in anyway by ropey, and its faster. Just use that.
    ropey::str_utils::char_to_byte_idx(s, char_pos)
}

pub fn split_at_char(s: &str, char_pos: usize) -> (&str, &str) {
    s.split_at(chars_to_bytes(s, char_pos))
}

#[inline]
#[allow(unused)]
pub fn consume_chars<'a>(content: &mut &'a str, len: usize) -> &'a str {
    let (here, remaining) = split_at_char(*content, len);
    *content = remaining;
    here
}

#[inline]
#[allow(unused)]
pub fn bytes_to_chars(s: &str, byte_pos: usize) -> usize {
    ropey::str_utils::byte_to_char_idx(s, byte_pos)
}

pub fn count_chars(s: &str) -> usize {
    ropey::str_utils::byte_to_char_idx(s, s.len())
}

#[cfg(test)]
mod test {
    use crate::unicount::*;

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
            let actual_bytes = std_chars_to_bytes(s, i);
            let ropey_bytes = ropey::str_utils::char_to_byte_idx(s, i);
            // dbg!(expected, actual);
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

