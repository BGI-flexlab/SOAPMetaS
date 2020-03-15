# "metaphlanDB" Folder Instruction

## Methods for generation of necessary files

+ Download `mpa_v294_CHOCOPhlAn_201901.tar` file from <https://bitbucket.org/biobakery/metaphlan2/downloads/>, decompress the file to obtain ".pkl" and ".fna" files.
+ Obtain `species2genome.txt` file. Users may follow [this instruction](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/utils/Species2genome_txt_generation.md) for help. We also provide an example here named `defined_species2genomes.txt`.
+ Download generation script: [marker\_matrix\_generate.py](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/utils/MetaPhlAn2_marker_matrix_generate.py)

Command:

```Bash
python3 meph_marker_matrix_generate.py --mpa-fna-bz2 mpa.fna --species2genome defined_species2genomes.txt --mpa-pickle mpa.pkl
```

The command will generate "MetaPhlAn2\_marker.matrix", "MetaPhlAn2\_mpa\_v20\_m200\_NewName.fna", "MetaPhlAn2\_Species\_genome\_gc.list", "MetaPhlAn2\_mpa.markers.list.json", "MetaPhlAn2\_mpa.taxonomy.list.json" files.

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

