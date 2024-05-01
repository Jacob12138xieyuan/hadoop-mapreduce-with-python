import io
import sys

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')

# Your code setting up data structures here if necessary (equivalent to setup() function)

for line in input_stream:
    # Your code operating on each line here (equivalent to map() function)
    name, score = line.strip().split()
    # (name, score_sum, count)
    print(f"{name}\t{score}\t1")

# Your code for post-processing here if necessary (equivalent to cleanup() function)
