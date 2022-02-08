
pub trait IteratorWithCtx<'a> {
    type Item;
    type Ctx: 'a;

    fn next_ctx(&mut self, ctx: Self::Ctx) -> Option<Self::Item>;
}

impl<I, Item> IteratorWithCtx<'static> for I where I: Iterator<Item=Item> {
    type Item = Item;
    type Ctx = ();

    fn next_ctx(&mut self, _ctx: Self::Ctx) -> Option<Self::Item> {
        self.next()
    }
}

// impl<Iter: IteratorWithCtx<Item=Item, Ctx=()>, Item> IntoIterator<Item=Item> for Iter {
//     type Item = Item;
//     type IntoIter = IterWithNoCtx<Item, Self>;
//
//     fn into_iter(self) -> Self::IntoIter {
//         IterWithNoCtx(self)
//     }
// }

// impl<Item> IteratorWithCtx where Self: IteratorWithCtx<Item=Item, Ctx=()> {
// impl IteratorWithCtx where Self: IteratorWithCtx<Ctx=()> {
//
// }

pub struct IterWithNoCtx<Item, Iter: IteratorWithCtx<'static, Item=Item, Ctx=()>>(Iter);

impl<Item, Iter: Iterator<Item=Item>> Iterator for IterWithNoCtx<Item, Iter> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_ctx(())
   }
}

// Ok:
// impl<Item> Iterator for dyn IteratorWithCtx<Item=Item, Ctx=()> {
//     type Item = Item;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self.next_ctx(())
//     }
// }