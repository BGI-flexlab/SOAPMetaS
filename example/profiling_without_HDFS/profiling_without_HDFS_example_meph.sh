#!/bin/bash
# File Name: comg_gene_profiling_example.sh


soapmetas=/path/to/SOAPMetasS.jar
sparksubmit=/path/to/spark-2.4.4-bin-hadoop2.7/bin/spark-submit #/path/to/spark-submit

sID=AnalysisNo
workDir=$(pwd -P)/profiling_without_HDFS/
processDir=${workDir}/process

outputHDFSDir=/path/to/HDFS/output/
tempDir=${processDir}/temp
if [ -d ${processDir} ]; then
	rm -r ${processDir}
fi
mkdir -p ${outputHDFSDir}
mkdir -p ${tempDir}

driverTmp=${tempDir}/driverTemp
alignmentTmp=${tempDir}/alignmentTemp

#### meph mode
databaseDir=../database
referenceIndex=${databaseDir}/metaphlanDB/referenceIndex/mpa_v20_m200 # bowtie2 index file prefix
refMatrix=${databaseDir}/metaphlanDB/MetaPhlAn2_marker.matrix
speciesGenoGC=${databaseDir}/metaphlanDB/MetaPhlAn2_Species_genome_gc.list
mpaMarkerList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.markers.list.json
mpaTaxonList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.taxonomy.list.json
excludeMarker=${databaseDir}/metaphlanDB/mpa_exclude_marker_NewName

masterURL=spark://hostname:7077
queueName=default
bowtieThread=2
alnExeNum=2
alnExeMem=3g
alnExeCores=${bowtieThread}
profExeNum=2
profExeMem=512m
profExeCores=2
profTaskCpus=1
partitionNumPerSample=2

bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${bowtieThread}"
bowtie2index=${referenceIndex}
inputSampleList=${workDir}/sample.list

#sparkOptions="--conf spark.task.cpus=${thread} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=512 --conf spark.executor.memoryOverhead=512 --driver-memory 1G --executor-memory 1G --executor-cores ${thread} --num-executors ${executorNum}"
sparkOptions="--master ${masterURL} --queue ${queueName} --deploy-mode client --driver-memory 1g --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=384 --conf spark.executor.memoryOverhead=2048"

#### meph mode
${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --local --prof-pipe meph -i ${inputSampleList} -x ${referenceIndex} -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" -ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --driver-tmp-dir ${driverTmp} --mpa-marker-list ${mpaMarkerList} --mpa-taxon-list ${mpaTaxonList} --mpa-exclude-list ${excludeMarker} --align-executor-memory ${alnExeMem} --align-executor-number ${alnExeNum} --align-executor-cores ${alnExeCores} --prof-executor-memory ${profExeMem} --prof-executor-number ${profExeNum} --prof-executor-cores ${profExeCores} --prof-task-cpus ${profTaskCpus} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e
