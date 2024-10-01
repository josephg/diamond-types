use crate::encoding::ChunkType;
use bumpalo::collections::vec::Vec as BumpVec;
use uuid::Uuid;
use crate::encoding::varint::{push_u32, push_u64, push_usize, try_push_u32, try_push_u64, try_push_usize};

pub(crate) trait TryExtendFromSlice {
    // The only error allowed here is a "out of space" error. I'm just going to return it as a naked
    // result because I think thats fine for now.
    fn try_extend_from_slice(&mut self, slice: &[u8]) -> Result<(), ()>;
}

pub(crate) trait ExtendFromSlice {
    fn extend_from_slice(&mut self, slice: &[u8]);
}

impl<T> TryExtendFromSlice for T where T: ExtendFromSlice {
    fn try_extend_from_slice(&mut self, slice: &[u8]) -> Result<(), ()> {
        self.extend_from_slice(slice);
        Ok(())
    }
}

// impl<T> ExtendFromSlice for T where T: ExtendFromSliceInfallable {
//     type Result = ();
//
//     fn extend_from_slice(&mut self, slice: &[u8]) -> Self::Result {
//         todo!()
//     }
// }

impl ExtendFromSlice for Vec<u8> {
    fn extend_from_slice(&mut self, slice: &[u8]) {
        Vec::extend_from_slice(self, slice);
    }
}

impl<'a> ExtendFromSlice for BumpVec<'a, u8> {
    fn extend_from_slice(&mut self, slice: &[u8]) {
        BumpVec::extend_from_slice(self, slice);
    }
}

/// This is a simple buffer for writing small data on the stack. Its similar to using a Vec<> or
/// something, but the StackWriteBuf will error if more bytes are ever written to it than it has
/// room for.
#[derive(Clone)]
pub(crate) struct StackWriteBuf<const SIZE: usize = 1024> {
    arr: [u8; SIZE],
    pos: usize,
}

impl<const SIZE: usize> Default for StackWriteBuf<SIZE> {
    fn default() -> Self {
        Self {
            arr: [0; SIZE],
            pos: 0,
        }
    }
}

impl<const SIZE: usize> StackWriteBuf<SIZE> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize { self.pos }
    pub fn is_empty(&self) -> bool { self.pos != 0 }

    pub fn data_slice(&self) -> &[u8] {
        &self.arr[..self.pos]
    }
}

impl<const SIZE: usize> TryExtendFromSlice for StackWriteBuf<SIZE> {
    fn try_extend_from_slice(&mut self, slice: &[u8]) -> Result<(), ()> {
        let end_pos = self.pos + slice.len();
        if end_pos > self.arr.len() || end_pos < self.pos { // Second condition here simplifies the asm.
            return Err(());
        }

        let target = &mut self.arr[self.pos..end_pos];
        target.copy_from_slice(slice);
        self.pos = end_pos;
        Ok(())
    }
}

pub(crate) fn try_push_str<V: TryExtendFromSlice>(into: &mut V, val: &str) -> Result<(), ()> {
    let bytes = val.as_bytes();
    try_push_usize(into, bytes.len())?;
    into.try_extend_from_slice(bytes)?;
    Ok(())
}
pub(crate) fn push_str<V: ExtendFromSlice>(into: &mut V, val: &str) {
    let bytes = val.as_bytes();
    push_usize(into, bytes.len());
    into.extend_from_slice(bytes);
}

pub(crate) fn push_uuid<V: ExtendFromSlice>(into: &mut V, val: Uuid) {
    into.extend_from_slice(val.as_bytes());
}

fn push_chunk_header<V: TryExtendFromSlice>(into: &mut V, chunk_type: ChunkType, len: usize) -> Result<(), ()> {
    try_push_u32(into, chunk_type as u32)?;
    try_push_usize(into, len)?;
    Ok(())
}

pub(crate) fn push_chunk<V: TryExtendFromSlice>(into: &mut V, chunk_type: ChunkType, data: &[u8]) -> Result<(), ()> {
    push_chunk_header(into, chunk_type, data.len())?;
    into.try_extend_from_slice(data)?;
    Ok(())
}

pub fn calc_checksum(data: &[u8]) -> u32 {
    // This is crc32c. Using the crc library because the resulting binary size is much smaller.
    // let checksum = crc32c::crc32c(&result);
    crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(data)
}

/// A DTSerializable object knows how to turn itself into a byte array.
pub(crate) trait DTSerializable {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S);
    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()>;

    fn to_stack_buf(&self) -> Result<StackWriteBuf, ()> {
        let mut buf: StackWriteBuf = Default::default();
        self.try_serialize(&mut buf)?;
        Ok(buf)
    }

    fn to_byte_vec(&self) -> Vec<u8> {
        let mut buf = vec![];
        self.serialize(&mut buf);
        buf
    }
}

impl DTSerializable for str {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        push_str(into, self);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        try_push_str(into, self)
    }
}

impl DTSerializable for usize {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        push_usize(into, *self);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        try_push_usize(into, *self)
    }
}

impl DTSerializable for u32 {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        push_u32(into, *self);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        try_push_u32(into, *self)
    }
}

impl DTSerializable for u64 {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        push_u64(into, *self);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        try_push_u64(into, *self)
    }
}

impl<A, B> DTSerializable for (A, B) where A: DTSerializable, B: DTSerializable {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        self.0.serialize(into);
        self.1.serialize(into);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        self.0.try_serialize(into)?;
        self.1.try_serialize(into)?;
        Ok(())
    }
}

impl<A, B, C> DTSerializable for (A, B, C) where A: DTSerializable, B: DTSerializable, C: DTSerializable {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        self.0.serialize(into);
        self.1.serialize(into);
        self.2.serialize(into);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        self.0.try_serialize(into)?;
        self.1.try_serialize(into)?;
        self.2.try_serialize(into)?;
        Ok(())
    }
}

impl<A, B, C, D> DTSerializable for (A, B, C, D) where A: DTSerializable, B: DTSerializable, C: DTSerializable, D: DTSerializable {
    fn serialize<S: ExtendFromSlice>(&self, into: &mut S) {
        self.0.serialize(into);
        self.1.serialize(into);
        self.2.serialize(into);
        self.3.serialize(into);
    }

    fn try_serialize<S: TryExtendFromSlice>(&self, into: &mut S) -> Result<(), ()> {
        self.0.try_serialize(into)?;
        self.1.try_serialize(into)?;
        self.2.try_serialize(into)?;
        self.3.try_serialize(into)?;
        Ok(())
    }
}

