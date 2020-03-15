# Marker database description

## IGC\_9.9M\_marker.matrix

[IGC](https://doi.org/10.1038/nbt.2942) gene information matrix. Needed by `--ref-matrix` option of SOAPMetaS.

content:

```Text
No.	geneName	geneLength	s__genus_species	GC_content
```

## Species\_genome\_gc.list

Information matrix of species in IGC. Needed by `--spe-gc` option of SOAPMetaS.

content:

```Text
s__genus_species	genomeLength	genomeGC_content	otherInformation
```

## IGC\_9.9M\_uniqGene.\*

Bowtie2 index files of [IGC gene sequence](http://meta.genomics.cn/meta/dataTools).

