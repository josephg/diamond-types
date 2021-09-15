use content_tree::ContentTree;
use rle::RleRun;

fn main() {
    let mut list = ContentTree::new();
    list.push(RleRun { val: false, len: 10 });

    list.insert_at_offset(5, RleRun { val: true, len: 2 });
    println!("List contains {:?}", list.iter().collect::<Vec<RleRun<bool>>>());
}