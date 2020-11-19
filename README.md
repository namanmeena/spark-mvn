# Spark Maven

A project structure for spark using scala and maven.

To build

```mvn clean install```

To run

```
spark-submit \
--master "local[2]" \
target/spark-test-1.0-SNAPSHOT-jar-with-dependencies.jar \
/tmp/student1
```