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

#### comg mode
referenceIndex=marker_data/reference_genome  # bowtie2 index file prefix
refMatrix=marker_data/reference_info.matrix
speciesGenoGC=marker_data/reference_genome_gc.list

masterURL=spark://hostname:7077
queueName=default
bowtieThread=2
alnExeNum=2
alnExeMem=1g
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
parkOptions="--master ${masterURL} --queue ${queueName} --deploy-mode client --driver-memory 1g --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=384 --conf spark.executor.memoryOverhead=2048"

#### comg mode
${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --local --prof-pipe comg --seq-mode se -i ${inputSampleList} -x ${bowtie2index} -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --driver-tmp-dir ${driverTmp} --align-executor-memory ${alnExeMem} --align-executor-number ${alnExeNum} --align-executor-cores ${alnExeCores} --prof-executor-memory ${profExeMem} --prof-executor-number ${profExeNum} --prof-executor-cores ${profExeCores} --prof-task-cpus ${profTaskCpus} --align-tmp-dir ${alignmentTmp} 1>${processDir}/running.o 2>${processDir}/running.e
