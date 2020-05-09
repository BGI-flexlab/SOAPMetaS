#!/bin/bash
# File Name: comg_gene_profiling_example.sh

sparksubmit = /path/to/spark-submit
soapmetas = /path/to/SOAPMetas.jar

databaseDir = example/database
HDFSWORKDIR = /HDFS/path/to/example
WORKDIR = example/meph_species_profiling
hdpDataDir = ${HDFSWORKDIR}/data
outputHDFSDir = ${HDFSWORKDIR}/results
localTempDir = ${WORKDIR}/temp

referenceIndex=${databaseDir}/metaphlanDB/referenceIndex/mpa_v20_m200 # bowtie2 index file prefix
refMatrix=${databaseDir}/metaphlanDB/MetaPhlAn2_marker.matrix
mpaMarkerList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.markers.list.json
mpaTaxonList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.taxonomy.list.json
excludeMarker=${databaseDir}/metaphlanDB/mpa_exclude_marker_NewName

masterURL = spark://hostname:7077
bowtieThread = 4
taskCores = 1
exeCores = 1
exeNum = 6
partitionNumPerSample=10

sparkOptions="--master ${masterURL} --deploy-mode client --driver-memory 2G --executor-memory 3G --conf spark.task.cpus=${taskCores} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=2048 --conf spark.executor.memoryOverhead=2048 --executor-cores ${exeCores} --num-executors ${exeNum}"

inputSampleList=${WORKDIR}/sample.list
bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${bowtieThread}"

${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --prof-pipe meph -i ${inputSampleList} -x ${referenceIndex} -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --tmp-local-dir ${localTempDir} --ref-matrix ${refMatrix} s--mpa-marker-list ${mpaMarkerList} --mpa-taxon-list ${mpaTaxonList} --mpa-exclude-list ${excludeMarker} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

