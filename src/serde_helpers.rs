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
#[derive(Clone, Debug, Eq, PartialEq)]
#[derive(Serialize, Deserialize)]
pub(crate) struct DTRangeTuple(usize, usize); // from, to.

impl From<DTRangeTuple> for DTRange {
    fn from(f: DTRangeTuple) -> Self {
        Self { start: f.0, end: f.1 }
    }
}
impl From<DTRange> for DTRangeTuple {
    fn from(range: DTRange) -> Self {
        DTRangeTuple(range.start, range.end)
    }
}




// impl<'de> Deserialize<'de> for TimeSpanRev {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
//         struct V;
//         impl Visitor for V {
//             type Value = TimeSpanRev;
//
//             fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//                 formatter.write_str("struct TimeSpanRev")
//             }
//
//             fn visit_seq<A>(self, seq: A) -> Result<Self::Value, serde::de::Error> where A: SeqAccess<'de> {
//
//             }
//         }
//
//         const FIELDS: &'static [&'static str] = &["start", "end", "fwd"];
//         deserializer.deserialize_struct("TimeSpanRev", FIELDS, V)
//     }
// }
