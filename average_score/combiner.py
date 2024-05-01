import sys

# Your code setting up data structures here if necessary (equivalent to setup() function)
table = {}

for line in sys.stdin:
    # Your code operating on each line here (equivalent to map() function)
    print(f'{line}', file=sys.stderr)
    name, score_sum, count = line.strip().split('\t')
    score_sum = int(score_sum)
    count = int(count)
    if name not in table:
        table[name] = [score_sum, count]
    else:
        table[name][0] += score_sum
        table[name][1] += count

# Your code for post-processing here if necessary (equivalent to cleanup() function)
for name, [score_sum, count] in table.items():
    print(f"{name}\t{score_sum}\t{count}")  
