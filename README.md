# SOAPMetas

## TEMP NOTE

Test command:

```shell
# Test for options
spark-submit --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar -h
```

```shell
# Test for alignment
spark-submit --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar -1 /home/metal/TEST/example/reads/reads_1.fq -2 /home/metal/TEST/example/reads/reads_2.fq -x /home/metal/TEST/example/index/genome --seq-mode pe -r "" -o /home/metal/TEST/SOAPMetas_Output -n 3 -e "--end-to-end --phred33 --no-discordant -X 1200 --no-hd --no-sq" --aln-tmp-dir /home/metal/TEST/SOAPMetas_tmp
```

```shell
spark-submit --packages org.apache.commons:commons-math3:3.6.1,org.renjin:renjin-script-engine:0.9.2719,com.github.samtools:htsjdk:2.18.1,org.seqdoop:hadoop-bam:7.10.0 --exclude-packages commons-cli:commons-cli,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth,org.apache.hadoop:hadoop-hdfs,org.apache.hadoop:hadoop-mapreduce-client-app,org.apache.hadoop:hadoop-mapreduce-client-common,org.apache.hadoop:hadoop-yarn-common,org.apache.hadoop:hadoop-yarn-api,org.codehaus.jackson:jackson-xc,org.apache.hadoop:hadoop-yarn-client,org.apache.hadoop:hadoop-mapreduce-client-core,org.apache.hadoop:hadoop-yarn-server-common,org.apache.hadoop:hadoop-mapreduce-client-shuffle,org.apache.hadoop:hadoop-mapreduce-client-jobclient,org.apache.hadoop:hadoop-yarn-server-nodemanager --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar -s /home/metal/TEST/multiSampleSAMList -x /home/metal/TEST/example/index/lambda_virus --seq-mode pe -r /home/metal/TEST/example/ref.matrix -o /home/metal/TEST/SOAPMetas_Output -n 4 --tmp-dir /home/metal/TEST/SOAPMetas_tmp --skip-alignment
```

