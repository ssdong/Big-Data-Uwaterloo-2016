This is a words correlation project which implements
PMI(Pointwise mutual information) between words in a given corpus(Still in progress).

First run Maven to build the package by executing the command

```
mvn clean package
```

under Project2 directory

Next run

```
hadoop jar target/bigdata2016w-0.1.0-SNAPSHOT.jar \
   ca.uwaterloo.cs.bigdata2016w.ssdong.project2.PairsPMI \
   -input data/Shakespeare.txt -output wc
```

under Project2 directory and you shall see the output
