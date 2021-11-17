use rle::{HasLength, MergableSpan, SplitableSpan};
use crate::localtime::TimeSpan;

#[derive(Copy, Clone, Debug, Eq)]
pub(super) struct Delete {
    /// The item *being* deleted
    pub target: TimeSpan,
    /// If target is `1..4` then we either delete `1,2,3` (rev=false) or `3,2,1` (rev=true).
    /// TODO: Consider swapping this, and making it a fwd variable (default true).
    pub rev: bool,
}

impl From<TimeSpan> for Delete {
    fn from(target: TimeSpan) -> Self {
        Delete {
            target,
            rev: false
        }
    }
}

impl PartialEq for Delete {
    fn eq(&self, other: &Self) -> bool {
        // Custom eq because if the two deletes have length 1, we don't care about rev.
        self.target == other.target && (self.rev == other.rev || self.target.len() <= 1)
    }
}

impl HasLength for Delete {
    fn len(&self) -> usize { self.target.len() }
}

impl SplitableSpan for Delete {
    fn truncate(&mut self, at: usize) -> Self {
        Delete {
            target: if self.rev {
                self.target.truncate_keeping_right(self.len() - at)
            } else {
                self.target.truncate(at)
            },
            rev: self.rev,
        }
    }
}

impl MergableSpan for Delete {
    fn can_append(&self, other: &Self) -> bool {
        // Can we append forward?
        let self_len_1 = self.len() == 1;
        let other_len_1 = other.len() == 1;
        if (self_len_1 || self.rev == false) && (other_len_1 || other.rev == false)
            && other.target.start == self.target.end {
            return true;
        }

        // Can we append backwards?
        if (self_len_1 || self.rev == true) && (other_len_1 || other.rev == true)
            && other.target.end == self.target.start {
            return true;
        }

        false
    }

    fn append(&mut self, other: Self) {
        self.rev = other.target.start < self.target.start;

        if self.rev {
            self.target.start = other.target.start;
        } else {
            self.target.end = other.target.end;
        }
    }
}

#[cfg(test)]
mod test {
    use rle::test_splitable_methods_valid;
    use super::*;

    #[test]
    fn split_fwd_rev() {
        let mut fwd = Delete {
            target: (1..4).into(),
            rev: false
        };
        assert_eq!(fwd.split(1), (
            Delete {
                target: (1..2).into(),
                rev: false
            },
            Delete {
                target: (2..4).into(),
                rev: false
            }
        ));

        let mut rev = Delete {
            target: (1..4).into(),
            rev: true
        };
        assert_eq!(rev.split(1), (
            Delete {
                target: (3..4).into(),
                rev: true
            },
            Delete {
                target: (1..3).into(),
                rev: true
            }
        ));
    }

    #[test]
    fn splitable_mergable() {
        test_splitable_methods_valid(Delete {
            target: (1..5).into(),
            rev: false
        });

        test_splitable_methods_valid(Delete {
            target: (1..5).into(),
            rev: true
        });
    }
}