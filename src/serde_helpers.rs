use serde::{Deserialize, Serialize, Serializer};
// use serde::de::{EnumAccess, Error, MapAccess, SeqAccess};
use serde::ser::SerializeStruct;
// use serde::de::Visitor;
use crate::rev_range::RangeRev;
use smartstring::alias::String as SmartString;
use crate::dtrange::DTRange;

#[cfg(feature = "serde")]
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
#[cfg(feature = "serde")]
impl Serialize for RangeRev {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        self.serialize_struct(serializer)
    }
}

#[cfg(feature = "serde")]
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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



// #[cfg(feature = "serde")]
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
