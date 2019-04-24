# SOAPMetas

## TEMP NOTE

Test command:

```shell
# Test for options
spark-submit --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar -h
```

```shell
# Test for alignment
spark-submit --repositories https://nexus.bedatadriven.com/content/groups/public/ --packages org.apache.commons:commons-math3:3.6.1,org.renjin:renjin-script-engine:0.9.2719,com.github.samtools:htsjdk:2.18.1,org.seqdoop:hadoop-bam:7.10.0 --exclude-packages commons-cli:commons-cli,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth,org.apache.hadoop:hadoop-hdfs,org.apache.hadoop:hadoop-mapreduce-client-app,org.apache.hadoop:hadoop-mapreduce-client-common,org.apache.hadoop:hadoop-yarn-common,org.apache.hadoop:hadoop-yarn-api,org.codehaus.jackson:jackson-xc,org.apache.hadoop:hadoop-yarn-client,org.apache.hadoop:hadoop-mapreduce-client-core,org.apache.hadoop:hadoop-yarn-server-common,org.apache.hadoop:hadoop-mapreduce-client-shuffle,org.apache.hadoop:hadoop-mapreduce-client-jobclient,org.apache.hadoop:hadoop-yarn-server-nodemanager --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar -i /home/metal/TEST/multiSampleFastqList -x /home/metal/TEST/example/index/lambda_virus --seq-mode pe -r /home/metal/TEST/example/ref.matrix -o /home/metal/TEST/SOAPMetas_Output -g /home/metal/TEST/example/species_gc.list -n 4 -e "--end-to-end --phred33 --no-discordant -X 1200" --tmp-dir /home/metal/TEST/SOAPMetas_tmp --skip-profiling
```

```shell
# Test for profiling
spark-submit --repositories https://nexus.bedatadriven.com/content/groups/public/ --packages org.apache.commons:commons-math3:3.6.1,org.renjin:renjin-script-engine:0.9.2719,com.github.samtools:htsjdk:2.18.1,org.seqdoop:hadoop-bam:7.10.0 --exclude-packages commons-cli:commons-cli,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth,org.apache.hadoop:hadoop-hdfs,org.apache.hadoop:hadoop-mapreduce-client-app,org.apache.hadoop:hadoop-mapreduce-client-common,org.apache.hadoop:hadoop-yarn-common,org.apache.hadoop:hadoop-yarn-api,org.codehaus.jackson:jackson-xc,org.apache.hadoop:hadoop-yarn-client,org.apache.hadoop:hadoop-mapreduce-client-core,org.apache.hadoop:hadoop-yarn-server-common,org.apache.hadoop:hadoop-mapreduce-client-shuffle,org.apache.hadoop:hadoop-mapreduce-client-jobclient,org.apache.hadoop:hadoop-yarn-server-nodemanager --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar -s /home/metal/TEST/multiSampleSAMList -x /home/metal/TEST/example/index/lambda_virus --seq-mode pe -r /home/metal/TEST/example/ref.matrix -o /home/metal/TEST/SOAPMetas_Output -g /home/metal/TEST/example/species_gc.list -n 4 --tmp-dir /home/metal/TEST/SOAPMetas_tmp --skip-alignment
```

```shell
#Test for GC Bias recalibration
spark-submit --repositories https://nexus.bedatadriven.com/content/groups/public/ --packages org.apache.commons:commons-math3:3.6.1,org.renjin:renjin-script-engine:0.9.2719,com.github.samtools:htsjdk:2.18.1,org.seqdoop:hadoop-bam:7.10.0 --exclude-packages commons-cli:commons-cli,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth,org.apache.hadoop:hadoop-hdfs,org.apache.hadoop:hadoop-mapreduce-client-app,org.apache.hadoop:hadoop-mapreduce-client-common,org.apache.hadoop:hadoop-yarn-common,org.apache.hadoop:hadoop-yarn-api,org.codehaus.jackson:jackson-xc,org.apache.hadoop:hadoop-yarn-client,org.apache.hadoop:hadoop-mapreduce-client-core,org.apache.hadoop:hadoop-yarn-server-common,org.apache.hadoop:hadoop-mapreduce-client-shuffle,org.apache.hadoop:hadoop-mapreduce-client-jobclient,org.apache.hadoop:hadoop-yarn-server-nodemanager --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar -s /home/metal/TEST/multiSampleSAMList -x /home/metal/TEST/example/index/lambda_virus --seq-mode pe -r /home/metal/TEST/example/ref.matrix -o /home/metal/TEST/SOAPMetas_Output -g /home/metal/TEST/example/species_gc.list -n 4 --tmp-dir /home/metal/TEST/SOAPMetas_tmp --skip-alignment --gc-cali --gc-model-file /home/metal/TEST/builtin_model.json
```

```shell
# Test for GC Bias model training
spark-submit --repositories https://nexus.bedatadriven.com/content/groups/public/ --packages org.apache.commons:commons-math3:3.6.1,org.renjin:renjin-script-engine:0.9.2719,com.github.samtools:htsjdk:2.18.1,org.seqdoop:hadoop-bam:7.10.0 --exclude-packages commons-cli:commons-cli,org.apache.hadoop:hadoop-common,org.apache.hadoop:hadoop-annotations,org.apache.hadoop:hadoop-auth,org.apache.hadoop:hadoop-hdfs,org.apache.hadoop:hadoop-mapreduce-client-app,org.apache.hadoop:hadoop-mapreduce-client-common,org.apache.hadoop:hadoop-yarn-common,org.apache.hadoop:hadoop-yarn-api,org.codehaus.jackson:jackson-xc,org.apache.hadoop:hadoop-yarn-client,org.apache.hadoop:hadoop-mapreduce-client-core,org.apache.hadoop:hadoop-yarn-server-common,org.apache.hadoop:hadoop-mapreduce-client-shuffle,org.apache.hadoop:hadoop-mapreduce-client-jobclient,org.apache.hadoop:hadoop-yarn-server-nodemanager --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas-0.0.1.jar -i /home/metal/TEST/multiSampleFastqList -x /home/metal/TEST/example/index/lambda_virus --seq-mode pe -r /home/metal/TEST/example/ref.matrix -o /home/metal/TEST/SOAPMetas_Output -g /home/metal/TEST/example/species_gc.list -n 4 --tmp-dir /home/metal/TEST/SOAPMetas_tmp --skip-alignment --gc-model-train --spe-fa /home/metal/TEST/example/lambda_virus.fa --zz-output-point
```