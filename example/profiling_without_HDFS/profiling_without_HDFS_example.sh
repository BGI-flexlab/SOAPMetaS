#!/bin/bash
# File Name: comg_gene_profiling_example.sh

WORKDIR=`pwd -P`
HDFSWORKDIR=${WORKDIR}
databaseDir=${WORKDIR}

sparksubmit=/path/to/spark-submit
soapmetas=/path/to/SOAPMetas-0.0.4-jar-with-dependencies.jar

hdpDataDir=${HDFSWORKDIR}/data
outputHDFSDir=${HDFSWORKDIR}/results
localTempDir=${WORKDIR}/temp

/bin/echo -e "SAMPLEID1\tSAMPLEID1\t${hdpDataDir}/read1.fastq" >sample.list
#/bin/echo -e "SAMPLEID2\tSAMPLEID2\t${hdpDataDir}/read2.fastq" >>sample.list

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

thread=1
exeNum=2
partitionNumPerSample=1
bowtie2options="--very-sensitive --phred33 --no-unal --xeq --threads ${thread}"
#sparkOptions="--conf spark.task.cpus=${thread} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=512 --conf spark.executor.memoryOverhead=512 --driver-memory 1G --executor-memory 1G --executor-cores ${thread} --num-executors ${exeNum}"
sparkOptions="--master local[*] --conf spark.task.cpus=${thread} --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=384 --driver-memory 1g --executor-memory 512m --executor-cores ${thread} --num-executors ${exeNum}"

inputSampleList=${WORKDIR}/sample.list

#### comg mode
${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --retain-temp --local --prof-pipe comg -i ${inputSampleList} -x ${referenceIndex} --seq-mode se -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --tmp-local-dir ${localTempDir} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

#### meph mode
#${sparksubmit} ${sparkOptions} --class org.bgi.flexlab.metas.SOAPMetas ${soapmetas} --local --prof-pipe meph -i ${inputSampleList} -x ${referenceIndex} --seq-mode se -o ${outputHDFSDir} -n ${partitionNumPerSample} -e "${bowtie2options}" --ana-lev species --ref-matrix ${refMatrix} --spe-gc ${speciesGenoGC} --tmp-local-dir ${localTempDir} --mpa-marker-list ${mpaMarkerList} --mpa-taxon-list ${mpaTaxonList} --mpa-exclude-list ${excludeMarker} 1>${WORKDIR}/running.o 2>${WORKDIR}/running.e

