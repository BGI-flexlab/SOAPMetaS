#!/bin/bash
# File Name: comg_gene_profiling_example.sh

WORKDIR=/path/to/example/comg_species_profiling
HDFSWORKDIR=/path/to/example/comg_species_profiling

databaseDir=/path/to/example/database

export SPARK_HOME=/path/to/spark-2.4.4-bin-hadoop2.7
sparksubmit=/path/to/spark-2.4.4-bin-hadoop2.7/bin/spark-submit #/path/to/spark-submit
soapmetas=/path/to/SOAPMetasS.jar

outputHDFSDir=${HDFSWORKDIR}/results
driverTmp=${WORKDIR}/temp

referenceIndex=${databaseDir}/marker_data/IGC_9.9M_uniqGene # bowtie2 index file prefix
refMatrix=${databaseDir}/marker_data/IGC_9.9M_marker.matrix
speciesGenoGC=${databaseDir}/marker_data/Species_genome_gc.list

inputSampleList=${WORKDIR}/sample.list

masterURL=spark://hostname:7077
bowtieThread=4
alnExeNum=4
alnExeMem=24g
alnExeCores=4
profExeNum=8
profExeMem=12g
profExeCores=2
profTaskCpus=1

partitionNumPerSample=10

bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${bowtieThread}"

sparkOptions="--master ${masterURL} --deploy-mode client --driver-memory 3g --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=384 --conf spark.executor.memoryOverhead=2048"

${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --prof-pipe comg -i ${inputSampleList} -x ${referenceIndex} --large-index -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --driver-tmp-dir ${driverTmp} --ana-lev markers --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --align-executor-memory ${alnExeMem} --align-executor-number ${alnExeNum} --align-executor-cores ${alnExeCores} --prof-executor-memory ${profExeMem} --prof-executor-number ${profExeNum} --prof-executor-cores ${profExeCores} --prof-task-cpus ${profTaskCpus} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

