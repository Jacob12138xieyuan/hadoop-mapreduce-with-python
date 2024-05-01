import sys

# Your code setting up data structures here if necessary (equivalent to setup() function)
table = {}

for line in sys.stdin:
    # Your code operating on each line here (more or less equivalent to reduce() function,
    # but only operating on a single tuple at a time), pairs are sorted by key
    print(f'{line}', file=sys.stderr)
    doc, collectionID = line.strip().split('\t')
    collectionID = int(collectionID)
    if doc not in table:
        table[doc] = [collectionID]
    else:
        table[doc] += [collectionID]

for doc, collectionIDs in table.items():
    if 0 in collectionIDs:
        print(f"{doc}\t0")
    if 1 in collectionIDs:
        print(f"{doc}\t1")
