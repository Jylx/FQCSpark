#!/bin/sh

#获取程序运行时间s
function getTiming(){
    start=$1
    end=$2

    start_s=`echo $start | cut -d '.' -f 1`
    start_ns=`echo $start | cut -d '.' -f 2`
    end_s=`echo $end | cut -d '.' -f 1`
    end_ns=`echo $end | cut -d '.' -f 2`

    time_micro=$(( (10#$end_s-10#$start_s)*1000000 + (10#$end_ns/1000 - 10#$start_ns/1000) ))
    time_ms=`expr $time_micro/1000  | bc `
    time_s=`expr $time_ms/1000  | bc `
#    time_minute=`expr $time_s/60  | bc `

#    echo "$time_micro microseconds"
#    echo "$time_ms ms"
    echo "运行时间= $time_s s"
#    echo "$time_minute minute"
}

#获取文件或文件夹大小MB
function getFileSize() {
  temp=$(du -sb $1 | awk '{print $1}')
  result=$(echo "scale=2; $temp / 1024 /1024" | bc)
  echo $1"文件大小= "${result}" MB"
}




#hadoop jar /opt/temp/fastdoopc/fastdoopc-1.0.0-all.jar /opt/temp/fastdoopc/uc.conf
inputPath=/home/luyao/documents/gene/fastq/danji/three-2
outputPath=/home/luyao/documents/gene/fastq/ds1
inputFiles=$(ls $path)
echo "----------------------开始一次压缩任务 inputPath:"${inputPath}" outputPath:"${outputPath}
begin_time=`date +%s.%N`
for filename in $inputFiles
do
#   subFileName=${filename:0:55}
   #echo ${subFileName}
   absoluteInputFileName=${path}"/"${filename}
   absoluteOutputFileName=${outputPath}"/"${filename}".spring"
   echo "===开始压缩 "${absoluteFileName}" -> "${absoluteOutputFileName}
   spring -c -i ${absoluteInputFileName} -o ${absoluteOutputFileName}
   echo "===结束压缩 "${absoluteFileName}" -> "${absoluteOutputFileName}
done
end_time=`date +%s.%N`
echo "----------------------结束一次压缩任务 inputPath:"${inputPath}" outputPath:"${outputPath}
echo "压缩结果信息:"
getTiming $begin_time $end_time
getFileSize $outputPath



#compressedFileName="/home/luyao/documents/gene/fastq/danji/three-2/HG00097.chrom11.ILLUMINA.bwa.GBR.low_coverage.20130415.fastq"









