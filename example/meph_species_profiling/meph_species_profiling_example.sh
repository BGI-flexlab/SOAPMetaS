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

referenceIndex=${databaseDir}/metaphlanDB/referenceIndex/mpa_v20_m200 # bowtie2 index file prefix
refMatrix=${databaseDir}/metaphlanDB/MetaPhlAn2_marker.matrix
mpaMarkerList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.markers.list.json
mpaTaxonList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.taxonomy.list.json
excludeMarker=${databaseDir}/metaphlanDB/mpa_exclude_marker_NewName

thread=4
exeNum=6
partitionNumPerSample=10
bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${thread}"
sparkOptions="--conf spark.task.cpus=${thread} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=2048 --conf spark.executor.memoryOverhead=2048 --driver-memory 4G --executor-memory 3G --executor-cores ${thread} --num-executors ${exeNum}"

inputSampleList=${WORKDIR}/sample.list

${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --prof-pipe meph -i ${inputSampleList} -x ${referenceIndex} --seq-mode se -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --tmp-local-dir ${localTempDir} --mpa-marker-list ${mpaMarkerList} --mpa-taxon-list ${mpaTaxonList} --mpa-exclude-list ${excludeMarker} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

