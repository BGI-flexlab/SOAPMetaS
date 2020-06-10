# "metaphlanDB" Folder Instruction

This instruction is based on the mpa\_v20\_m200 version (old pickle) of MetaPhlAn2 database. New version (mpa\_v294, new pickle) of the database contains more genome information. Our script should be compatible with these two version unless the data structure in pickle file changes.

## Methods for generation of necessary files

+ Download `mpa_v20_m200.tar` file from <https://www.dropbox.com/sh/7qze7m7g9fe2xjg/AADHWzATSQcI0CNFD0sk7MAga>, decompress the file to obtain ".pkl" and ".fna" files.
+ Obtain `species2genome.txt` file from <https://github.com/biobakery/MetaPhlAn/blob/master/utils/species2genomes.txt>. Users may follow [this instruction](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/utils/Species2genome_txt_generation.md) to generate the ultimate `species2genome` file, and we also provide an example file here named `defined_species2genomes.txt`.
+ Download generation script: [MetaPhlAn2\_marker\_matrix\_generate.py](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/utils/MetaPhlAn2_marker_matrix_generate.py)

Command:

```Bash
python3 MetaPhlAn2_marker_matrix_generate.py --mpa-fna-bz2 mpa.fna --species2genome defined_species2genomes.txt --mpa-pickle mpa.pkl --old-pickle # delete this options for new version
```

The command will generate "MetaPhlAn2\_marker.matrix", "MetaPhlAn2\_mpa\_v20\_m200\_NewName.fna.bz2", "MetaPhlAn2\_mpa.markers.list.json", "MetaPhlAn2\_mpa.taxonomy.list.json" files.

## File content

+ MetaPhlAn2\_marker.matrix

```Text
No.	markerName	geneLength	s__genus_species_xxx	GC_content
```

+ MetaPhlAn2\_Species\_genome\_gc.list

```Text
s__genus_species_xxx	genomeLength	genomeGC_content	accessionID(GCF/GCA_000xxx)
```

+ \*.list.json

JSON format of `.pkl` pickle file.

+ NewName.fna

Re-named FASTA of mpa marker sequence.

+ mpa\_exclude\_marker\_NewName

Name list of excluded marker genes. These genes can be found in metaphlan2 source code. We renamed them.

+ \*.bt2

Bowtie2 index of mpa marker sequence. Run `bowtie2-build MetaPhlAn2_mpa_v20_m200_NewName.fna mpa_v20_m200` to generate.

