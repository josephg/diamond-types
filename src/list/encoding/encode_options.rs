use crate::list::ListOpLog;
use crate::LV;

// TODO: Make a builder API for this
#[derive(Debug, Clone)]
pub struct EncodeOptions<'a> {
    pub(crate) user_data: Option<&'a [u8]>,

    // NYI.
    // from_version: LocalVersion,

    pub(crate) store_start_branch_content: bool,

    /// Experimental.
    pub(crate) store_end_branch_content: bool,

    pub(crate) store_inserted_content: bool,
    pub(crate) store_deleted_content: bool,

    pub(crate) compress_content: bool,

    pub(crate) verbose: bool,
}


pub const ENCODE_PATCH: EncodeOptions = EncodeOptions {
    user_data: None,
    store_start_branch_content: false,
    store_end_branch_content: false,
    store_inserted_content: true,
    store_deleted_content: false,
    compress_content: true,
    verbose: false
};

pub const ENCODE_FULL: EncodeOptions = EncodeOptions {
    user_data: None,
    store_start_branch_content: true,
    store_end_branch_content: false,
    store_inserted_content: true,
    store_deleted_content: false, // ?? Not sure about this one!
    compress_content: true,
    verbose: false
};

impl<'a> Default for EncodeOptions<'a> {
    fn default() -> Self {
        ENCODE_FULL
    }
}

// pub struct EncodeOptionsBuilder<'a>(EncodeOptions<'a>);

impl<'a> EncodeOptions<'a> {
    pub fn patch() -> Self {
        ENCODE_PATCH.clone()
    }
    pub fn full() -> Self {
        ENCODE_FULL.clone()
    }

    pub fn encode_from(&self, oplog: &ListOpLog, from_version: &[LV]) -> Vec<u8> {
        oplog.encode_from(self, from_version)
    }

    pub fn user_data(mut self, data: &'a [u8]) -> Self {
        self.user_data = Some(data);
        self
    }

    pub fn store_start_branch_content(mut self, store_start_branch_content: bool) -> Self {
        self.store_start_branch_content = store_start_branch_content;
        self
    }

    pub fn experimentally_store_end_branch_content(mut self, store: bool) -> Self {
        self.store_end_branch_content = store;
        self
    }

    pub fn store_inserted_content(mut self, store_inserted_content: bool) -> Self {
        self.store_inserted_content = store_inserted_content;
        self
    }

    pub fn store_deleted_content(mut self, store_deleted_content: bool) -> Self {
        self.store_deleted_content = store_deleted_content;
        self
    }

    pub fn compress_content(mut self, compress_content: bool) -> Self {
        self.compress_content = compress_content;
        self
    }

    pub fn verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn build(self) -> EncodeOptions<'a> {
        self
    }
}

pub type EncodeOptionsBuilder<'a> = EncodeOptions<'a>;