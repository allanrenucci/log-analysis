# Hadoop log analyser

The overall goal of this application written in Scala, Spark, and MLLIB, is to predicts application failures based on their log data. 

My solution is composed of two modules: a parser `LogParser` and a log analyser `LogAnalysis`.
I parse 5 types of logs which will account for 6 features used for the machine learning part.

Features:
- Duration: I compute the difference between the start time and the end time of an application.
- Allocated Container: I compute the number of allocated containers per application.
- Killed Container: I compute the number of killed containers per application.
- Succeeded Container: I compute the number of containers exited with success. (This one seems redundant with the previous one. So I don’t use it).
- Memory footprint (Accounts for 2 features): I compute the ratio of physical and virtual memory used out of the total available.

The log analyser works as follow:
First it reads each log line and parse it. The undefined log lines are then filtered out.
Then the logs are grouped by application id.

For each application id and set of logs, I build a set of labeled points (label + features). The points with no label are filtered out.

Then I use a decision tree for my model. The decision tree is very well fitted for binary classification. I also tried linear models such as SVM but the results were not convincing.
