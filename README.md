# hadoop-mapreduce-with-python

## Hadoop Streaming

Hadoop Streaming is a utility that allows you to use **any programming language** to write MapReduce jobs for Hadoop. It provides a way to process data in Hadoop using **standard input and output streams**, making it flexible and language-agnostic.

With Hadoop Streaming, you can write your MapReduce tasks in languages like **Python**, Ruby, Perl, or even shell scripts

## Case1: word_count

Input file

```
hello hello
hadoop so good
hello world
hadoop good
hell world
hadoop spark
hello spark spark
```

Write MapReduce algo to count the words in file. Output these doc as <word, count>

### mapper

```
%%file mapper.py
import io
import sys

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')

# Your code setting up data structures here if necessary (equivalent to setup() function)

for line in input_stream:
    # Your code operating on each line here (equivalent to map() function)
    for word in line.strip().split():
        # (word, 1)
        print(f"{word}\t1")

# Your code for post-processing here if necessary (equivalent to cleanup() function)
```

### reducer

```
%%file reducer.py
import sys

# Your code setting up data structures here if necessary (equivalent to setup() function)
table = {}

for line in sys.stdin:
    # Your code operating on each line here (equivalent to map() function)
    print(f'{line}', file=sys.stderr)
    word, count = line.strip().split('\t')
    count = int(count)
    if word not in table:
        table[word] = count
    else:
        table[word] += count

# Your code for post-processing here if necessary (equivalent to cleanup() function)
for word, count in table.items():
    print(f"{word}\t{count}")
```

## Case2: doc_in_collection

Input file

```
0 hellohello
0 hadoop
0 hadoop
1 hello
1 hadoop
0 hell
0 hadoop
0 hello
1 hadoop
```

There are two collections 0 and 1, write MapReduce algo to check if a certain "doc" is in both collections. Output these doc as <doc, 1>

### mapper

```
%%file mapper.py
import io
import sys

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')

# Your code setting up data structures here if necessary (equivalent to setup() function)

for line in input_stream:
    # Your code operating on each line here (equivalent to map() function)
    collectionID, doc = line.strip().split()
    print(f"{doc}\t{collectionID}")

# Your code for post-processing here if necessary (equivalent to cleanup() function)
```

### reducer

```
%%file reducer.py
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

# Your code for post-processing here if necessary (equivalent to cleanup() function)
for doc, collectionIDs in table.items():
    if 0 in collectionIDs and 1 in collectionIDs:
        print(f"{doc}\t1")
```
