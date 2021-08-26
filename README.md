# FQCSpark
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
FQCSparkJarName
RefFileName
targetDir
hdfsDir
localDir
-1 
linespermap
```

## decompress

```shell
java -classpath FQCSparkJarName decompress.entrance.DecompressBySparkMain RefFileName compressedFileName decompressDir
```

