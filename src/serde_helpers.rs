use std::fmt;
use std::fmt::Formatter;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{MapAccess, SeqAccess, Visitor};
// use serde::de::{EnumAccess, Error, MapAccess, SeqAccess};
use serde::ser::SerializeStruct;
// use serde::de::Visitor;
use crate::rev_range::RangeRev;
use smartstring::alias::String as SmartString;
use crate::dtrange::DTRange;
use std::range::Range;

pub(crate) trait FlattenSerializable {
    fn struct_name() -> &'static str;
    fn num_serialized_fields() -> usize;
    fn serialize_fields<S>(&self, s: &mut S::SerializeStruct) -> Result<(), S::Error> where S: Serializer;

    fn serialize_struct<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut s = serializer.serialize_struct(Self::struct_name(), Self::num_serialized_fields())?;
        self.serialize_fields::<S>(&mut s)?;
        s.end()
    }
}

// I can't use the default flattening code because bleh.
impl Serialize for RangeRev {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.serialize_struct(serializer)
    }
}

// This is sooo looong. I just wanted RangeRev.span to be inlined :(
impl<'de> Deserialize<'de> for RangeRev {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field { Start, End, Fwd }

        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = RangeRev;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct RangeRev")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<RangeRev, V::Error>
                where
                    V: SeqAccess<'de>,
            {
                let start = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let end = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let fwd = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(RangeRev { span: (start..end).into(), fwd })
            }

            fn visit_map<V>(self, mut map: V) -> Result<RangeRev, V::Error>
                where
                    V: MapAccess<'de>,
            {
                let mut start = None;
                let mut end = None;
                let mut fwd = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Start => {
                            if start.is_some() {
                                return Err(de::Error::duplicate_field("start"));
                            }
                            start = Some(map.next_value()?);
                        }
                        Field::End => {
                            if end.is_some() {
                                return Err(de::Error::duplicate_field("end"));
                            }
                            end = Some(map.next_value()?);
                        }
                        Field::Fwd => {
                            if fwd.is_some() {
                                return Err(de::Error::duplicate_field("fwd"));
                            }
                            fwd = Some(map.next_value()?);
                        }
                    }
                }
                let start = start.ok_or_else(|| de::Error::missing_field("start"))?;
                let end = end.ok_or_else(|| de::Error::missing_field("end"))?;
                let fwd = fwd.ok_or_else(|| de::Error::missing_field("fwd"))?;
                Ok(RangeRev { span: (start..end).into(), fwd })
            }
        }

        const FIELDS: &'static [&'static str] = &["start", "end", "fwd"];
        deserializer.deserialize_struct("Duration", FIELDS, DurationVisitor)
        // deserializer.deserialize_struct("RangeRev", &["start", "end", "fwd"], RangeRevVisitor)
    }
}

impl FlattenSerializable for RangeRev {
    fn struct_name() -> &'static str {
        "TimeSpanRev"
    }

    fn num_serialized_fields() -> usize {
        3
    }

    fn serialize_fields<S>(&self, s: &mut S::SerializeStruct) -> Result<(), S::Error> where S: Serializer {
        s.serialize_field("start", &self.span.start)?;
        s.serialize_field("end", &self.span.end)?;
        s.serialize_field("fwd", &self.fwd)?;
        Ok(())
    }
}

/// This is used to flatten `[agent, seq]` into a tuple for serde serialization.
// #[derive(Clone, Debug, Eq, PartialEq)]
// #[derive(Serialize, Deserialize)]
// pub(crate) struct DTRangeTuple(usize, usize); // from, to.
//
// impl From<DTRangeTuple> for DTRange {
//     fn from(f: DTRangeTuple) -> Self {
//         Self { start: f.0, end: f.1 }
//     }
// }
// impl From<DTRange> for DTRangeTuple {
//     fn from(range: DTRange) -> Self {
//         DTRangeTuple(range.start, range.end)
//     }
// }

#[derive(Serialize, Deserialize)]
pub struct RangeTuple(#[serde(with = "range")] pub Range<usize>);

impl From<RangeTuple> for Range<usize> {
    fn from(value: RangeTuple) -> Self {
        value.0
    }
}

impl From<Range<usize>> for RangeTuple {
    fn from(value: Range<usize>) -> Self {
        RangeTuple(value)
    }
}

pub mod range {
    use ::smallvec::SmallVec;
    use super::*;

    pub fn serialize<S: Serializer>(r: &Range<usize>, s: S) -> Result<S::Ok, S::Error> {
        (r.start, r.end).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Range<usize>, D::Error> {
        let (start, end) = <(usize, usize)>::deserialize(d)?;
        Ok(Range { start, end })
    }

    // This is a generic wrapper that lets us serialize / deserialze to / from lists of ranges.
    // Horrible that this is needed, but eh.
    pub struct RangeSlice<Seq>(pub Seq);

    impl<'a> From<RangeSlice<&'a [Range<usize>]>> for &'a [Range<usize>] {
        fn from(value: RangeSlice<&'a [Range<usize>]>) -> Self {
            value.0
        }
    }
    impl<'a> From<&'a [Range<usize>]> for RangeSlice<&'a [Range<usize>]> {
        fn from(value: &'a [Range<usize>]) -> Self {
            RangeSlice(value)
        }
    }

    impl<'a, Seq: ?Sized> Serialize for RangeSlice<&'a Seq>
        where &'a Seq: IntoIterator<Item=&'a Range<usize>>,
    {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.collect_seq(self.0.into_iter().map(|r| RangeTuple(*r)))
        }
    }

    // I'm specialising here over Smallvec because smallvec has visitor code.
    // I could instead impl this for any Extend<...> but that would need a custom visitor.
    impl<'de, const N: usize> Deserialize<'de> for RangeSlice<SmallVec<Range<usize>, N>> {
        fn deserialize<D>(d: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
            smallvec::deserialize(d).map(RangeSlice)
        }
    }

    pub mod opt {
        use super::*;

        pub fn serialize<S: Serializer>(v: &Option<Range<usize>>, s: S) -> Result<S::Ok, S::Error> {
            v
                .as_ref()
                .map(|r| RangeTuple(*r)).serialize(s)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Range<usize>>, D::Error> {
            Ok(
                Option::<RangeTuple>::deserialize(d)?
                    .map(|RangeTuple(r)| r)
            )
        }
    }

    pub mod smallvec {
        use super::*;
        use ::smallvec::SmallVec;
        pub fn serialize<S, const N: usize>(v: &SmallVec<Range<usize>, N>, s: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
            s.collect_seq(v.iter().map(|r| RangeTuple(*r)))
        }

        pub fn deserialize<'de, D, const N: usize>(d: D) -> Result<SmallVec<Range<usize>, N>, D::Error>
        where D: Deserializer<'de>
        {
            Ok(
                SmallVec::<RangeTuple, N>::deserialize(d)?
                    .into_iter()
                    .map(|RangeTuple(r)| r)
                    .collect()
            )
        }
    }
}
