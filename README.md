# FQCSpark

## install

- install BSC，and configure environment variables
- install JDK-1.8.0，Hadoop-3.1.3, and Spark-2.1.1

## package

```shell
cd projectPath
mvn assembly:assembly
```

## compress
```shell
bin/spark-submit \
--class compress.util.SparkApp \
--master yarn \
--deploy-mode client \
--driver-memory xg \
--executor-memory yg \
--executor-cores m \
--num-executors n \
FQCSparkJarName \
RefFileName \
targetDir \
hdfsDir \
localDir \
-1 \
linespermap
```

## decompress

```shell
java -classpath FQCSparkJarName decompress.entrance.DecompressBySparkMain RefFileName compressedFileName decompressDir
```

