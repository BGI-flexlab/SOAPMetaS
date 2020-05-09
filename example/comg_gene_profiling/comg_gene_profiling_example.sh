#!/bin/bash
# File Name: comg_gene_profiling_example.sh

sparksubmit = /path/to/spark-submit
soapmetas = /path/to/SOAPMetas.jar

databaseDir = example/database
HDFSWORKDIR = /HDFS/path/to/example
WORKDIR = example/comg_gene_profiling
hdpDataDir = ${HDFSWORKDIR}/data
outputHDFSDir = ${HDFSWORKDIR}/results
localTempDir = ${WORKDIR}/temp

referenceIndex = ${databaseDir}/marker_data/IGC_9.9M_uniqGene # bowtie2 index file prefix
refMatrix = ${databaseDir}/marker_data/IGC_9.9M_marker.matrix
speciesGenoGC = ${databaseDir}/marker_data/Species_genome_gc.list

masterURL = spark://hostname:7077
bowtieThread = 4
taskCores = 1
exeCores = 1
exeNum = 6
partitionNumPerSample=10

sparkOptions="--master ${masterURL} --deploy-mode client --driver-memory 4G --executor-memory 24G --conf spark.task.cpus=${taskCores} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=2048 --conf spark.executor.memoryOverhead=5120 --executor-cores ${exeCores} --num-executors ${exeNum}"

inputSampleList=${WORKDIR}/sample.list
bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${bowtieThread}"

${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --prof-pipe comg -i ${inputSampleList} -x ${referenceIndex} --large-index -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --tmp-local-dir ${localTempDir} --ana-lev markers --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

