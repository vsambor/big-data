#!/bin/bash
sbt package
scp -P 2222 target/scala-2.11/spam_filter_2.11-0.1-SNAPSHOT.jar root@localhost:/tmp/spam_filter_2.11-0.1-SNAPSHOT.jar

