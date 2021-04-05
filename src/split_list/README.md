# Split list

This is a simple data structure for efficiently storing run-length-encoded node pointers in a way which allows these operations to be fast:

- Append to the end of the list
- Look up an item by index
- Edit any entry, which might result in an entry being split

This is used for marker_tree node pointers and storing historical transactions by their order numbers.