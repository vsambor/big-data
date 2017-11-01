## Big Data Spark Project

### Build
To build the job run the following command:

```
sbt package
```

### Deploy
To deploy the job run the following command:

```
spark-submit --class "SpamFilter" /tmp/spam_filter_2.11-0.1-SNAPSHOT.jar
```
