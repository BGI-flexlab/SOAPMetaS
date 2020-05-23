#!/bin/bash
# File Name: comg_gene_profiling_example.sh

WORKDIR=/path/to/example/profiling_without_HDFS
HDFSWORKDIR=/path/to/example/profiling_without_HDFS

databaseDir=${WORKDIR}

export SPARK_HOME=/path/to/spark-2.4.4-bin-hadoop2.7
sparksubmit=/path/to/spark-2.4.4-bin-hadoop2.7/bin/spark-submit #/path/to/spark-submit
soapmetas=/path/to/SOAPMetasS.jar

hdpDataDir=${HDFSWORKDIR}/data
outputHDFSDir=${HDFSWORKDIR}/results
driverTmp=${WORKDIR}/temp

printf "SAMPLEID1\tSAMPLEID1\t${hdpDataDir}/read1.fastq\n" >${WORKDIR}/sample.list
printf "SAMPLEID2\tSAMPLEID2\t${hdpDataDir}/read2.fastq" >>${WORKDIR}/sample.list

#### comg mode
referenceIndex=${databaseDir}/marker_data/reference_genome  # bowtie2 index file prefix
refMatrix=${databaseDir}/marker_data/reference_info.matrix
speciesGenoGC=${databaseDir}/marker_data/reference_genome_gc.list

#### meph mode
#referenceIndex=${databaseDir}/metaphlanDB/referenceIndex/mpa_v20_m200 # bowtie2 index file prefix
#refMatrix=${databaseDir}/metaphlanDB/MetaPhlAn2_marker.matrix
#speciesGenoGC=${databaseDir}/metaphlanDB/MetaPhlAn2_Species_genome_gc.list
#mpaMarkerList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.markers.list.json
#mpaTaxonList=${databaseDir}/metaphlanDB/MetaPhlAn2_mpa.taxonomy.list.json
#excludeMarker=${databaseDir}/metaphlanDB/mpa_exclude_marker_NewName

masterURL=spark://hostname:7077
bowtieThread=1

#alnExeNum=4
#alnExeMem=1g
#alnExeCores=1

#profExeNum=2
#profExeMem=512m
#profExeCores=2
#profTaskCpus=1

exeCores=1
#executorNum=2 # --num-executors is only for YARN
partitionNumPerSample=2
bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${bowtieThread}"
#sparkOptions="--conf spark.task.cpus=${thread} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=512 --conf spark.executor.memoryOverhead=512 --driver-memory 1G --executor-memory 1G --executor-cores ${thread} --num-executors ${executorNum}"
sparkOptions="--master ${masterURL} --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores ${exeCores} --conf spark.task.cpus=${exeCores} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=384"

inputSampleList=${WORKDIR}/sample.list

#### comg mode
${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --local --prof-pipe comg -i ${inputSampleList} -x ${referenceIndex} --seq-mode se -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --driver-tmp-dir ${driverTmp} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

#### meph mode
#${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --local --prof-pipe meph -i ${inputSampleList} -x ${referenceIndex} --seq-mode se -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --driver-tmp-dir ${driverTmp} --mpa-marker-list ${mpaMarkerList} --mpa-taxon-list ${mpaTaxonList} --mpa-exclude-list ${excludeMarker} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e
