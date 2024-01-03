/// This module was ported & adapted from some older OT code, to instead work with the traversal
/// data structure. It needs to be cleanly merged into traversal, and the tests need to be
/// reinstated.

// TODO: Move this file into a separate crate.
use smartstring::alias::{String as SmartString};
use smallvec::SmallVec;

use crate::list::TraversalComponent;
use TraversalComponent::*;
use crate::list::ot::traversal::TraversalOp;
use rle::AppendRle;
use crate::unicount::chars_to_bytes;
use crate::list::ot::editablestring::EditableText;

impl TraversalComponent {
    pub fn is_noop(&self) -> bool { self.len() == 0 }

    // TODO: Replace calls with truncate().
    pub fn slice(&self, offset: usize, len: usize) -> TraversalComponent {
        debug_assert!(self.len() >= offset + len);
        match *self {
            Retain(_) => Retain(len),
            Del(_) => Del(len),
            // Move to slice_chars when available
            // https://doc.rust-lang.org/1.2.0/std/primitive.str.html#method.slice_chars
            Ins { content_known, .. } => Ins { len, content_known },
        }
    }
}

impl TraversalOp {
    // This is a very imperative solution. Maybe a more elegant way of doing
    // this would be to return an iterator to the resulting document... which
    // then you could collect() to realise into a new string.
    pub fn apply<D: EditableText>(&self, doc: &mut D) {
        let mut pos = 0usize;
        let mut s = self.content.as_str();

        for c in &self.traversal {
            match c {
                Retain(n) => pos += *n as usize,
                Del(len) => doc.remove_at(pos, *len as usize),
                Ins { len, content_known } => {
                    if *content_known {
                        doc.insert_at(pos, take_first_chars(&mut s, *len as usize));
                    } else {
                        let content = SmartString::from("x").repeat(*len as usize);
                        doc.insert_at(pos, content.as_str());
                    }

                    pos += *len as usize;
                }
            }
        }
    }
}

// ***** Transform & Compose code


#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Context { Pre, Post }

impl TraversalComponent {
    // How much space this element takes up in the string before the op
    // component is applied
    fn ctx_len(&self, ctx: Context) -> usize {
        match ctx {
            Context::Pre => self.pre_len(),
            Context::Post => self.post_len(),
        }
    }
}

struct TextOpIterator<'a> {
    op: &'a [TraversalComponent],

    ctx: Context,
    idx: usize,
    offset: usize,
}

// I'd love to use a normal rust iterator here, but we need to pass in a limit
// parameter each time we poll the iterator.
impl <'a>TextOpIterator<'a> {
    fn next(&mut self, max_size: usize) -> TraversalComponent {
        // The op has an infinite skip at the end.
        if self.idx == self.op.len() { return Retain(max_size); }

        let c = &self.op[self.idx];
        let clen = c.ctx_len(self.ctx);

        if clen == 0 {
            // The component is invisible in the context.
            // TODO: Is this needed?
            assert_eq!(self.offset, 0);
            self.idx += 1;

            // This is non ideal - if the compnent contains a large string we'll
            // clone the string here. We could instead pass back a reference,
            // but then the slices below will need to deal with lifetimes or be
            // Rc or something.
            *c
        } else if clen - self.offset <= max_size {
            // Take remainder of component.
            let result = c.slice(self.offset, clen - self.offset);
            self.idx += 1;
            self.offset = 0;
            result
        } else {
            // Take max_size of the component.
            let result = c.slice(self.offset, max_size);
            self.offset += max_size;
            result
        }
    }
}

// By spec, text operations never end with (useless) trailing skip components.
fn trim(traversal: &mut SmallVec<[TraversalComponent; 2]>) {
    while let Some(Retain(_)) = traversal.last() {
        traversal.pop();
    }
}

fn traversal_iter(traversal: &[TraversalComponent], ctx: Context) -> TextOpIterator {
    TextOpIterator { op: traversal, ctx, idx: 0, offset: 0 }
}

fn append_remainder_component(traversal: &mut SmallVec<[TraversalComponent; 2]>, mut iter: TextOpIterator) {
    loop {
        let chunk = iter.next(usize::MAX);
        if chunk == Retain(usize::MAX) { break; }
        traversal.push_rle(chunk);
    }
}

fn append_remainder(op: &mut TraversalOp, mut iter: TextOpIterator, mut content: &str) {
    loop {
        let chunk = iter.next(usize::MAX);
        match chunk {
            Retain(usize::MAX) => { break; }
            Ins { len, content_known: true } => {
                op.content.push_str(take_first_chars(&mut content, len as usize));
            }
            _ => {}
        }
        op.traversal.push_rle(chunk);
    }
    assert!(content.is_empty());
}

/// Transform the positions in one traversal component by another. Produces the replacement
/// traversal.
///
/// This operates on lists of TraversalComponents because the inserted content is unaffected.
pub fn transform(op: &[TraversalComponent], other: &[TraversalComponent], is_left: bool) -> SmallVec<[TraversalComponent; 2]> {
    // debug_assert!(op.is_valid() && other.is_valid());

    let mut result = SmallVec::<[TraversalComponent; 2]>::new();
    let mut iter = traversal_iter(op, Context::Pre);

    for c in other {
        match c {
            Retain(mut len) => { // Skip. Copy input to output.
                while len > 0 {
                    let chunk = iter.next(len);
                    len -= chunk.pre_len();
                    result.push_rle(chunk);
                }
            },

            Del(mut len) => {
                while len > 0 {
                    let chunk = iter.next(len);
                    len -= chunk.pre_len();

                    // Discard all chunks except for inserts.
                    if let Ins { len, content_known } = chunk {
                        result.push_rle(Ins { len, content_known });
                    }
                }
            },

            Ins { len, .. } => { // Write a corresponding skip.
                // Left's insert should go first.
                if is_left {
                    let next = iter.next(0);
                    if !next.is_noop() { result.push_rle(next); }
                }

                // Skip the text that otherop inserted.
                result.push_rle(Retain(*len));
            },
        }
    }

    append_remainder_component(&mut result, iter);
    trim(&mut result);
    // debug_assert!(result.is_valid());

    result
}

fn skip_chars(s: &str, num: usize) -> &str {
    let byte_offset = chars_to_bytes(s, num);
    &s[byte_offset..]
}

fn take_first_chars<'a>(s: &mut &'a str, count: usize) -> &'a str {
    let num_bytes = chars_to_bytes(s, count);
    let (first, remainder) = s.split_at(num_bytes);
    // result.content.push_str(first);
    *s = remainder;
    first
}

/// Compose two traversals together. This operates on the traversals themselves because the inserted
/// strings may be modified as a result. (Eg if the first operation inserts, and the second deletes
/// the newly inserted content).
///
/// Note transform is not closed under compose. See this document for more detail:
/// https://github.com/ottypes/text-unicode/blob/master/NOTES.md
pub fn compose(a: &TraversalOp, b: &TraversalOp) -> TraversalOp {
    // debug_assert!(a.is_valid() && b.is_valid());

    let mut result = TraversalOp::new();
    let mut iter = traversal_iter(&a.traversal, Context::Post);
    let mut a_content = a.content.as_str();
    let mut b_content = b.content.as_str();

    for c in &b.traversal {
        match c {
            Retain(mut len) => {
                // Copy len from a.
                while len > 0 {
                    let chunk = iter.next(len);
                    len -= chunk.post_len();
                    if let Ins { len, content_known: true } = &chunk {
                        // Copy content.
                        result.content.push_str(take_first_chars(&mut a_content, *len as usize));
                    }
                    result.traversal.push_rle(chunk);
                }
            },

            Del(mut len) => {
                // Skip len items in a.
                while len > 0 {
                    let chunk = iter.next(len);
                    len -= chunk.post_len();
                    // An if let .. would be better here once stable.
                    match chunk {
                        Retain(n) | Del(n) => { result.traversal.push_rle(Del(n)); },
                        Ins { len, content_known: true } => {
                            // Cancel inserts.
                            a_content = skip_chars(a_content, len as usize);
                        }
                        _ => {}
                    }
                }
            },

            Ins { len, content_known } => {
                result.traversal.push_rle(Ins { len: *len, content_known: *content_known });
                if *content_known {
                    result.content.push_str(take_first_chars(&mut b_content, *len as usize));
                    // take_from(&mut b_content, *len as usize, &mut result);
                }
            }
        }
    }


    append_remainder(&mut result, iter, a_content);
    trim(&mut result.traversal);

    debug_assert!(b_content.is_empty());

    if cfg!(debug_assertions) {
        result.check();
    }

    result
}


#[cfg(test)]
mod tests {
    use std::io::*;
    use json_minimal::Json;
    use crate::list::ot::traversal::*;
    use TraversalComponent::*;
    use rle::AppendRle;
    use std::fs::File;
    use crate::list::ot::ot::{compose, transform};

    #[test]
    fn simple_apply() {
        let op = TraversalOp::new_insert(2, "hi");
        let mut doc = "yo".to_string();
        op.apply(&mut doc);
        assert_eq!(doc, "yohi");
    }

    struct JsonStreamIter<T>(Lines<BufReader<T>>);

    fn read_json<'a>(filename: &str) -> JsonStreamIter<impl Read> {
        let file = File::open(filename).unwrap();
        let reader = BufReader::new(file);
        JsonStreamIter(reader.lines())
    }

    impl<T: Read> Iterator for JsonStreamIter<T> {
        type Item = Json;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next().and_then(|line| {
                let line = line.unwrap();
                if line.len() == 0 { None }
                else { Some(Json::parse(&line.as_bytes()).unwrap()) }
            })
        }
    }

    fn get<'a>(val: &'a Json, field: &str) -> &'a Json {
        unwrap_obj_value(val.get(field).unwrap())
    }

    // This is gross. The other option would be to use serde or the 'json' crate, either of which
    // would give us less code here but a slower time compiling. Eh.
    fn unwrap_array(val: &Json) -> &Vec<Json> {
        if let Json::ARRAY(arr) = val { arr }
        else { panic!("Does not contain array") }
    }

    fn unwrap_obj_value(val: &Json) -> &Json {
        if let Json::OBJECT { value, .. } = val {
            value.unbox()
        } else { panic!("Does not contain obj") }
    }

    fn unwrap_number(val: &Json) -> f64 {
        if let Json::NUMBER(num) = val {
            *num
        } else { panic!("Does not contain number") }
    }

    fn unwrap_string(val: &Json) -> &str {
        if let Json::STRING(str) = val {
            str.as_str()
        } else { panic!("Does not contain string") }
    }

    fn json_to_op(val: &Json) -> TraversalOp {
        let arr = unwrap_array(val);

        let mut result = TraversalOp::new();
        for m in arr {
            result.traversal.push_rle(match m {
                Json::NUMBER(n) => Retain(*n as usize),
                Json::STRING(s) => {
                    result.content.push_str(s);
                    Ins {
                        len: s.chars().count(),
                        content_known: true
                    }
                }
                Json::JSON(_) => {
                    let d = unwrap_obj_value(m.get("d").unwrap());
                    Del(unwrap_number(d) as usize)
                }
                _ => panic!("Invalid data {:?}", m)
            });
        }

        result.check();
        result
    }

    #[test]
    fn fuzz_compose() {
        for (_i, val) in read_json("../../test_data/ot/compose.json").enumerate() {
            // println!("i {}", _i);
            let op1 = json_to_op(get(&val, "op1"));
            let op2 = json_to_op(get(&val, "op2"));
            let expected = json_to_op(get(&val, "result"));

            let actual = compose(&op1, &op2);
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn fuzz_transform() {
        for (_i, val) in read_json("../../test_data/ot/transform.json").enumerate() {
            // println!("i {}", _i);
            let op = json_to_op(get(&val, "op"));
            let other_op = json_to_op(get(&val, "otherOp"));
            let side_is_left = unwrap_string(get(&val, "side")) == "left";
            let expected = json_to_op(get(&val, "result")).traversal;

            let result = transform(&op.traversal, &other_op.traversal, side_is_left);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn fuzz_apply() {
        for (_i, val) in read_json("../../test_data/ot/apply.json").enumerate() {
            // println!("i {}", _i);
            let mut str = unwrap_string(get(&val, "str")).to_string();
            let op = json_to_op(get(&val, "op"));
            let expected = unwrap_string(get(&val, "result"));

            op.apply(&mut str);
            assert_eq!(str, expected);
            // dbg!(str, op, result);
        }
    }
}

