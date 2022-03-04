use std::ops::Deref;

/// This is a simple iterator wrapper which has a buffer, and allows an item to be "put back" on
/// the iterator.

#[derive(Debug, Clone)]
pub(crate) struct BufferedIter<Iter: Iterator> {
    inner: Iter,
    buffer: Option<Iter::Item>,
}

impl<Iter: Iterator> Iterator for BufferedIter<Iter> {
    type Item = Iter::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let buffered = self.buffer.take();
        if buffered.is_some() {
            buffered
        } else {
            self.inner.next()
        }
    }
}

impl<Iter: Iterator> BufferedIter<Iter> {
    pub fn new(inner: Iter) -> Self {
        Self {
            inner,
            buffer: None
        }
    }

    pub fn push_back(&mut self, item: Iter::Item) {
        assert!(self.buffer.is_none());
        self.buffer = Some(item);
    }
}

impl<Iter: Iterator> From<Iter> for BufferedIter<Iter> {
    fn from(iter: Iter) -> Self {
        Self::new(iter)
    }
}

impl<Iter: Iterator> Deref for BufferedIter<Iter> {
    type Target = Iter;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) trait Buffered: Iterator + Sized {
    fn buffered(self) -> BufferedIter<Self> {
        self.into()
    }
}

impl<T: Iterator> Buffered for T {}