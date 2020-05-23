#!/bin/bash
# File Name: comg_gene_profiling_example.sh

WORKDIR=/path/to/example/meph_species_profiling
HDFSWORKDIR=/path/to/example/meph_species_profiling

databaseDir=/path/to/example/database

export SPARK_HOME=/path/to/spark-2.4.4-bin-hadoop2.7
sparksubmit=/path/to/spark-2.4.4-bin-hadoop2.7/bin/spark-submit #/path/to/spark-submit
soapmetas=/path/to/SOAPMetasS.jar

outputHDFSDir=${HDFSWORKDIR}/results
driverTmp=${WORKDIR}/temp

referenceIndex=${databaseDir}/metaphlanDB/referenceIndex/mpa_v20_m200 # bowtie2 index file prefix
refMatrix=${databaseDir}/metaphlanDB/MetaPhlAn2_marker.matrix
mpaMarkerList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.markers.list.json
mpaTaxonList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.taxonomy.list.json
excludeMarker=${databaseDir}/metaphlanDB/mpa_exclude_marker_NewName

inputSampleList=${WORKDIR}/sample.list

masterURL=spark://hostname:7077
bowtieThread=3
alnExeNum=4
alnExeMem=3g
alnExeCores=3
profExeNum=6
profExeMem=2g
profExeCores=2
profTaskCpus=1

partitionNumPerSample=10

bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${bowtieThread}"

sparkOptions="--master ${masterURL} --deploy-mode client --driver-memory 3g --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=384 --conf spark.executor.memoryOverhead=2048"

${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --prof-pipe meph -i ${inputSampleList} -x ${referenceIndex} -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --driver-tmp-dir ${driverTmp} --ref-matrix ${refMatrix} --mpa-marker-list ${mpaMarkerList} --mpa-taxon-list ${mpaTaxonList} --mpa-exclude-list ${excludeMarker} --align-executor-memory ${alnExeMem} --align-executor-number ${alnExeNum} --align-executor-cores ${alnExeCores} --prof-executor-memory ${profExeMem} --prof-executor-number ${profExeNum} --prof-executor-cores ${profExeCores} --prof-task-cpus ${profTaskCpus} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

