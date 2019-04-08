# SOAPMetas

## TEMP NOTE

Test command:

```shell
# Test for options
spark-submit --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar -h
```

```shell
# Test for single-sample alignment
#spark-submit --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar -1 /path/to/fq1 -2 /path/to/fq2 -x /path/to/index_prefix --seq-mode pe -r /no/need/for/alignment/test -o /absolute/path/to/outputdir -n 3 -e "--end-to-end --phred33 --no-discordant -X 1200 --no-hd --no-sq" [--aln-tmp-dir /absolute/path/to/temp]
spark-submit --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar -1 /home/metal/TEST/example/reads/reads_1.fq -2 /home/metal/TEST/example/reads/reads_2.fq -x /home/metal/TEST/example/index/genome --seq-mode pe -r "" -o /home/metal/TEST/SOAPMetas_Output -n 3 -e "--end-to-end --phred33 --no-discordant -X 1200 --no-hd --no-sq" --aln-tmp-dir /home/metal/TEST/SOAPMetas_tmp
```

