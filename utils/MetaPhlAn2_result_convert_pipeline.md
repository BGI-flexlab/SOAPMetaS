# MetaPhlAn2 Result Converter Pipeline

## Pre-requisite script

+ buildIDlists.pl which can generate species-genomeID mapping file. <https://github.com/CAMI-challenge/docker_profiling_tools/blob/master/metaphlan2/buildIDlists.pl>
+ convert_metaphlan2.pl which can convert the file format of profiling result.
+ Utils.pm <https://github.com/CAMI-challenge/docker_profiling_tools>
+ YAMLsj.pm <https://github.com/CAMI-challenge/docker_profiling_tools>

## Pre-requisite data

+ Original species2genomes.txt: <https://bitbucket.org/biobakery/metaphlan2/src/default/utils/species2genomes.txt>
+ Assembly Summary (genebank): <ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt>
+ Assembly Summary (refseq): <ftp://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt>
+ NCBI taxdump: <ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz>
+ Mannually collected bioproject-taxid file: <https://github.com/CAMI-challenge/docker_profiling_tools/blob/master/metaphlan2/workdir/manual.txt>
+ XML file from NCBI batch entrez system whilch contains taxon information of input bioproject accession number.

All files and introduction can be found in <https://github.com/CAMI-challenge/docker_profiling_tools/blob/master/metaphlan2/buildIDlists.pl>

## Input file

+ orig_profiling.abun: Profiling result file generated from MetaPhlAn2 (-t rel_ab) or SOAPMetaS (--prof-pipe meph --output-format DEFAULT).

## Step

All steps follow <https://github.com/CAMI-challenge/docker_profiling_tools/blob/master/metaphlan2/Dockerfile_metaphlan2> and comments in pre-requisite scripts.

1. Save all scripts and ".pm" modules in the same directory.
2. Save all pre-requisite data into one directory (we name it "workDir"), and decompress taxadump file in situ.
3. Run `perl buildIDlists.pl species2genomes.txt workDir 1>mappingresults.txt 2>running.e`
4. If there are still unknown/undefined accession ID, delete them from species2genomes.txt file.
5. Re-run the command in step 3.
6. Obtain mappingresults.txt.
7. For sample "SAMPLE_NAME", Run `perl convert_metaphlan2.pl mappingresults.txt orig_profiling.abun workDir SAMPLE_NAME > SAMPLE_NAME_CAMIFORM.abun`

