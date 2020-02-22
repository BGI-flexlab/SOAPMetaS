#!/bin/bash
# File Name: ProfilingConverter.sh
# Author: heshixu, heshixu@genomics.cn
# Created Time: Sat 2019-08-10 17:51:02 HKT
start_time=`date +'%Y-%m-%d %H:%M:%S'`
echo "process started at: $start_time"
start_seconds=$(date --date="$start_time" +%s)

ROOTDIR=${USERROOTDIR}
TESTDIR=${ROOTDIR}/SOAPMetas_TEST/Version0.0.2_TEST

PERLLIB=${TESTDIR}/tool
NCBIDUMPDIR=${TESTDIR}/database/species_data/
SOAPMetasAbunTaxIDConverter=${TESTDIR}/tool/AbundanceTaxIDConverter.py
converter_motu=${TESTDIR}/tool/convert_motu.pl
converter_metaphlan=${TESTDIR}/tool/convert_metaphlan2.pl

mergeddmp=${TESTDIR}/database/species_data/merged.dmp
namesdmp=${TESTDIR}/database/species_data/names.dmp
nodesdmp=${TESTDIR}/database/species_data/nodes.dmp

SOAPMetasSelectedTaxIDList=${TESTDIR}/database/species_data/SelectedMarkerSpecies.list
metaphlan_mappingResults=${TESTDIR}/database/metaphlan_map/mappingresults.txt

workDir=

for sample in ??
do
sampleID=
originalAbun=

# SOAPMetas
#python3 ${SOAPMetasAbunTaxIDConverter} ${originalAbun} ${SOAPMetasSelectedTaxIDList} ${sampleID} #output: SOAPMetas_profiling_motuFormat_${sampleID}.abundance
#perl -I ${PERLLIB} ${converter_motu} SOAPMetas_profiling_motuFormat_${sampleID}.abundance ${NCBIDUMPDIR} ${sampleID} >${workDir}/SOAPMetas_${sampleID}_CAMIFORM.profile

# MetaPhlAn2
#perl -I ${PERLLIB} ${converter_metaphlan} ${metaphlan_mappingResults} ${originalAbun} ${NCBIDUMPDIR} ${sampleID} >MetaPhlAn2_${sampleID}_CAMIFORM.profile

# mOTUsv1
# originalAbun: find resultDir/ -name \*insert.mm.dist.among.unique.scaled.taxaid.gz
# taxonomic.profiles/RefMG.v1.padded/reads.extracted.mOTU.v1.padded.solexaqa.solexaqa.allbest.l45.p97/0.taxonomic.profile.reads.extracted.mOTU.v1.padded.solexaqa.on.RefMG.v1.padded.solexaqa.allbest.l45.p97.insert.mm.dist.among.unique.scaled.taxaid.gz 
# The file need manual modification
#perl -I ${PERLLIB} ${converter_motu} ${originalAbun} ${NCBIDUMPDIR} ${sampleID} >${workDir}/mOTUsv1_${sampleID}_CAMIFORM.profile
done

end_time=`date +'%Y-%m-%d %H:%M:%S'`
end_seconds=$(date --date="$end_time" +%s)
echo "Total time "$((${end_seconds}-${start_seconds}))"s. Process ended at: $end_time"
