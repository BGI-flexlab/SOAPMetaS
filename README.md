# SOAPMetas


## About MetaPhlAn2

The marker name in default database of MetaPhlAn2 contains special character that can't be recognized by htsjdk.

注意, MetaPhlAn2 已经更新到2019版, 代码变动较大, 多了一些参数, 因此后续测试的时候需要考虑这些新参数的配置. 

当前测试中的 `01.profiling/HMP_HMASM_MetaWUGSCStool/SOAPMetas_MEPHProcessM2` 目录下的脚本, bowtie2 的比对参数已经调整与 MetaPhlAn2 一致. 

与此同时, 为了保证新参数与 SOAPMetas 一致， `01.profiling/HMP_HMASM_MetaWUGSCStool/metaphlan2` 目录下的脚本 `--read_min_len` 参数设置为 0 .

metaphlan2018 和 metaphlan2019 的区别包括: 

+ 2019 的脚本中删除了 markers_to_exclude 基因列表, 这些基因是在 2018.12.11 月的 commit d24f233 中提交的
+ 2019 的数据集更新了 marker 的名称
+ 2019 的数据集取消了 Viruses 的 marker
+ 2019 的数据集 fna 序列文件中增加了 marker 的 UniRef 信息以及 taxonomy 信息和 genome GCA 编号
+ 2019 的数据集 pkl 文件的 marker 部分取消了 t__ 级别的分类单位, 即所有 marker 的 clade 信息最低为 s__ 级别
+ 2019 的数据集 pkl 文件的 taxonomy 部分添加了 taxid 信息, 读入文件后与原 genolen 形成二元组
+ 2019 的脚本中增加了对 taxid 处理的支持, 增加了对 CAMI 标准格式的支持, 增加了处理比对 reads 数量的直接统计
+ 2019 的脚本中增加了对二次比对 (secondary alignment) 的过滤和对比对质量分数 MAPQ 的过滤条件
+ 2019 的脚本中增加了部分控制参数, 修改了部分非核心代码的逻辑

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