# Marker database description

## IGC\_9.9M\_marker.matrix

[IGC](https://doi.org/10.1038/nbt.2942) gene information matrix. Needed by `--ref-matrix` option of SOAPMetaS.

content:

```Text
No._of_gene	geneName	geneLength	s__genus_species	GC_content
```

## Species\_genome\_gc.list

Information matrix of species in IGC. Needed by `--spe-gc` option of SOAPMetaS.

content:

```Text
s__genus_species	genomeLength	genomeGC_content	otherInformation
```

## IGC\_9.9M\_uniqGene.\*

Bowtie2 index files of [Integrated non-redundant gene catalog](http://meta.genomics.cn/meta/dataTools), download link is <ftp://parrot.genomics.cn/gigadb/pub/10.5524/100001_101000/100064/1.GeneCatalogs/IGC.fa.gz>

