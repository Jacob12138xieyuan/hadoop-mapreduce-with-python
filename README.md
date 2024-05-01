# hadoop-mapreduce-with-python

## Hadoop Streaming

Hadoop Streaming is a utility that allows you to use **any programming language** to write MapReduce jobs for Hadoop. It provides a way to process data in Hadoop using **standard input and output streams**, making it flexible and language-agnostic.

With Hadoop Streaming, you can write your MapReduce tasks in languages like **Python**, Ruby, Perl, or even shell scripts

### Installation

```
brew install hadoop
```

core-site.xml

```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

hadoop-env.sh

```
export JAVA_HOME=/Users/neteasegames/Library/Java/JavaVirtualMachines/corretto-11.0.21/Contents/Home
export HADOOP_HOME=/opt/homebrew/Cellar/hadoop/3.4.0/libexec
```

hdfs-site.xml

```
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

mapred-site.xml

```
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
</configuration>
```

yarn-site.xml

```
<configuration>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>True</value>
    <final>false</final>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
</configuration>
```

### Start cluster

```
start-dfs.sh && start-yarn.sh && mapred --daemon start historyserver
```

### Web UI

HDFS node manager
http://localhost:9870/dfshealth.html#tab-overview
YARN resource manager (yarn mode)
http://localhost:8088/cluster
Job history (yarn mode)
http://localhost:19888/jobhistory

### Run Hadoop Streaming job

```
hadoop jar /opt/homebrew/Cellar/hadoop/3.4.0/libexec/share/hadoop/tools/lib/hadoop-streaming-3.4.0.jar \
-input path/to/input/file \
-output path/to/output/folder \
-file path/to/'mapper.py' \
-file path/to/'reducer.py' \
-file path/to/'combiner.py' \
-mapper 'path/to/python mapper.py' \
-reducer 'path/to/python reducer.py' \
-combiner 'path/to/python combiner.py'
```

### Close cluster

```
mapred --daemon stop historyserver && stop-yarn.sh && stop-dfs.sh
```

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

Write MapReduce algo to count the words in file. Output pair as <word, count>

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

## Case3: average_score

Input file

```
jacob 88
jacob 89
yanlin 79
tom 92
jacob 82
yanlin 89
john 65
john 87
jacob 95
yanlin 91
tommy 77
```

Write MapReduce algo to calculate the average score of each student. Output avg score as <name, score>

### mapper

```
%%file mapper.py
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
    print(f"{name}\t{score_sum/count}")
```
