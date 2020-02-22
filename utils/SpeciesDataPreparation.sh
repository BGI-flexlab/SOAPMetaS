#!/bin/bash
# File Name: assemblipath.sh
# Author: heshixu, heshixu@genomics.cn
# Created Time: Sun 2019-04-28 15:54:34 HKT
start_time=`date +'%Y-%m-%d %H:%M:%S'`
echo "process started at: $start_time"
start_seconds=$(date --date="$starttime" +%s)

ROOTDIR=${USERROOTDIR}
TESTDIR=${ROOTDIR}/SOAPMetas_TEST/Version0.0.2_TEST
TOOLDIR=${TESTDIR}/tool

pubDBDir=${PUBDBDIR}

linkScript=${TOOLDIR}/StdSpeciesListLink.py

workDir=${TESTDIR}/database/species_data
refseqDB=${workDir}/assembly_summary_refseq.txt
genbankDB=${workDir}/assembly_summary_genbank.txt

#curl -C - -O -sSL ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt
##curl -C - -O -sSL ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt
#curl -C - -O -sSL ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz

# Create species list
#awk -F "\t" '{print $1"\t"$5"\t"$8"\t"$9"\t"$20"\t"$6"\t"$7}' assembly_summary.txt |sed 's/ftp:\//'${pubDBDir}'/' |sed 's/ncbi.nlm.nih/ncbi.nih/' |sed '1d' >spe_std.list

#awk -F "\t" '{print $4}' IGC_9.9M_marker.matrix |uniq |sort |uniq >IGC9.9M_species_List

echo "marker species list needs manual modification. For species that has name in special format, we should split and re-combine the undefined name, and all the candidate name, seperated with comma mark, are in the same line."
echo "example: [Species] name --> [Species] name,Species name"
echo "         Species name1/name2 --> Species name1/name2,Species name1,Species name2"

marker_species_list=${workDir}/manual_IGC9.9M_species_List
python3 ${linkScript} ${refseqDB} ${genbankDB} ${marker_species_list} ${PUBDBDIR}/ftp.ncbi.nih.gov

# Check report.txt file
#for gcf in `awk -F "\t" '{print $2}' ouou`; do grep -vE "^#" $(grep ${gcf} spe_std.list| awk -F "\t" '{print $6"/*report.txt"}')| sed 's/^/'${gcf}'\t/'; done > ououou

awk '{$1=""; print $0}' Species_genome_gc.list |awk -F "," '{$2=""; print $0}' |sed 's/,/\t/g' |sort -k3,3 |uniq |sort -k2,2n> uniq_accession_list

end_time=`date +'%Y-%m-%d %H:%M:%S'`
end_seconds=$(date --date="$endtime" +%s)
echo "Total time "$((end_seconds-start_seconds))"s. Process ended at: $end_time"
