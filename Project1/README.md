This is a testing word count project retrieved from
https://github.com/lintool/bespin/blob/master/src/main/java/io/bespin/java/mapreduce/wordcount/WordCount.java

First run Maven to build the package by executing the command

```
mvn clean package
```

under Project1 directory

Next run

```
hadoop jar target/bigdata2016w-0.1.0-SNAPSHOT.jar \
   ca.uwaterloo.cs.bigdata2016w.ssdong.project1.WordCount \
   -input data/Shakespeare.txt -output wc
```

under Project1 directory and you shall see the output
