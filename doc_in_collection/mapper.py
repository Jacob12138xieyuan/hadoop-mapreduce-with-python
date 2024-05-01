import io
import sys

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')

# Your code setting up data structures here if necessary (equivalent to setup() function)

for line in input_stream:
    # Your code operating on each line here (equivalent to map() function)
    collectionID, doc = line.strip().split()
    print(f"{doc}\t{collectionID}")

# Your code for post-processing here if necessary (equivalent to cleanup() function)
