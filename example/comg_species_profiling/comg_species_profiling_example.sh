#!/bin/bash
# File Name: comg_gene_profiling_example.sh

WORKDIR=./
HDFSWORKDIR=/HDFS/path/to/example/
projectDir=../../
databaseDir=${projectDir}/example/database

sparksubmit=/path/to/spark-submit
soapmetas=${projectDir}/target/SOAPMetas-0.0.1-jar-with-dependencies.jar

hdpDataDir=${HDFSWORKDIR}/data
outputHDFSDir=${HDFSWORKDIR}/results
localTempDir=${WORKDIR}/temp

referenceIndex=${databaseDir}/marker_data/IGC_9.9M_uniqGene # bowtie2 index file prefix
refMatrix=${databaseDir}/marker_data/IGC_9.9M_marker.matrix
speciesGenoGC=${databaseDir}/marker_data/Species_genome_gc.list

thread=4
exeNum=6
partitionNumPerSample=10
bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${thread}"
sparkOptions="--conf spark.task.cpus=${thread} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=2048 --conf spark.executor.memoryOverhead=5120 --driver-memory 8G --executor-memory 24G --executor-cores ${thread} --num-executors ${exeNum}"

inputSampleList=${WORKDIR}/sample.list

${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --prof-pipe comg -i ${inputSampleList} -x ${referenceIndex} --seq-mode se -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --tmp-local-dir ${localTempDir} --large-index 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

