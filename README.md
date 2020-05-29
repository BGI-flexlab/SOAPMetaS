# SOAPMetaS

An Apache Spark<sup>TM</sup> based tool for profiling large metagenome datasets accurately on distributed cluster.

- [SOAPMetaS](#soapmetas)
  - [1. Prerequisites](#1-prerequisites)
    - [1.1 Hardware](#11-hardware)
    - [1.2 Software](#12-software)
    - [1.3 Reference Dataset](#13-reference-dataset)
      - ["meph" mode reference (recommended for new microbe communities where MetaPhlAn2 is preferable)](#meph-mode-reference-recommended-for-new-microbe-communities-where-metaphlan2-is-preferable)
      - ["comg" mode reference (recommended for new samples of known microbe communities which has known gene set)](#comg-mode-reference-recommended-for-new-samples-of-known-microbe-communities-which-has-known-gene-set)
  - [2. Important Options (alphabetical oder)](#2-important-options-alphabetical-oder)
  - [3. Usage](#3-usage)
    - [3.1 Simple command](#31-simple-command)
    - [3.2 IMPORTANT NOTES BEFORE RUNNING](#32-important-notes-before-running)
    - [3.3 Advanced usage](#33-advanced-usage)
  - [4. Examples](#4-examples)
  - [5. Build from Source](#5-build-from-source)
    - [5.1 Build Bowtie2 Native Library](#51-build-bowtie2-native-library)
      - [1) Bowtie2 library prerequisites](#1-bowtie2-library-prerequisites)
      - [2) Building Bowtie2 library](#2-building-bowtie2-library)
    - [5.2 Generate SOAPMetaS Jar Package](#52-generate-soapmetas-jar-package)
  - [6. Known Issues](#6-known-issues)
  - [7. License](#7-license)
  - [8. Acknowledgments](#8-acknowledgments)

## 1. Prerequisites

### 1.1 Hardware

Distributed cluster or any single node with **enough computing resource** (for multiple bowtie2 process with index files as large as human genome).

### 1.2 Software

+ Necessary
  + Java (1.8 tested)
  + Spark (2.4.4 tested)
  + Hadoop (2.7 tested)

<br/>

+ Optional
  + Maven 3.2.5 (for building from source)
  + YARN latest (for distributed system)
  + HDFS latest (for distributed file storage)

<br/>

SOAPMetaS has been tested in the environments of local, Spark standalone cluster as well as YARN cluster. Users should [download Spark](https://archive.apache.org/dist/spark/) and use `spark-submit` file to launch SOAPMetaS.

### 1.3 Reference Dataset

#### "meph" mode reference (recommended for new microbe communities where MetaPhlAn2 is preferable)

Including five (types of) files:

+ reference matrix
+ mpa markers json file
+ mpa taxonomy json file
+ MetaPhlAn2 excluded marker list
+ MetaPhlAn2 markers reference sequence index for Bowtie2

File demos are listed in `example/database/metaphlanDB` folder, refer to related [README](https://github.com/BGI-flexlab/SOAPMetaS/tree/dev_maple/example/database/metaphlanDB/README.md) for more information.

#### "comg" mode reference (recommended for new samples of known microbe communities which has known gene set)

Including three (types of) files:

+ reference matrix
+ species genome gc list
+ reference sequence index for Bowtie2

File demos are listed in `example/database/marker_data` folder, refer to related [README](https://github.com/BGI-flexlab/SOAPMetaS/tree/dev_maple/example/database/marker_data/README.md) for more information.

## 2. Important Options (alphabetical oder)

```Text
--align-executor-cores
Executor core number in alignment process. The config in Spark is spark.executor.cores/--executor-cores , we make it more flexible. Default: 1".

--align-executor-memory
Executor memory in alignment process. The value is related to the size of bowtie index file. Refer to the README in SOAPOMetas repo for more information. Example: 2g, 1600m, etc. Default: 2g".

--align-executor-number
Executor number in alignment process. Since alignment step doesn't support multi-tasks on one executor, user should adjust both this option and "-n" option for better performance. Refer to the README in SOAPOMetas repo for more information. Default: 3.

--align-tmp-dir
Local temp directory on executor nodes. The directory is used for storing intermediate tab5, sam and alignment log files. Default is /tmp/SOAPMetaS_Align_temp . Users should delete the directory manually if SOAPMetaS is finished or interrupted. For example: ssh <nodeIP> "rm -r /tmp/SOAPMetaS_Align_temp".

--ana-lev
The cluster level of output profiling result, including "marker" level for gene profiling and "species" level for species profiling. The option work with "--prof-pipe comg". Default: species.

-e, --extra-arg
Other options for Bowtie2. Please refer to Bowtie2 manual (http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml) for more information. All options should be enclosed together with quotation marks "". Default: "--very-sensitive --no-unal".

-g, --spe-gc
Genome information (genome length, genome GC) of each species in reference data. The file is used with "--ana-lev species" and "--prof-pipe comg". File format (tab delimited):
s__Genusname_speciesname	<Genome_Length>	<Genome_GC_rate>
s__Escherichia_coli	4641652	0.508
...

-i, --multi-sample-list
Multiple-sample clean FASTQ files list, one line per sample. The option is exclusive to "-s". Note that ReadGroupID info is not considered in current version. File format (tab delimited):
ReadGroupID1 Sample(SMTag)1 /path/to/read1_1.fq [/path/to/read1_2.fq]
ReadGroupID2 Sample(SMTag)2 /path/to/read2_1.fq [/path/to/read2_2.fq]
...

--iden-filt
Whether to filter alignment results by identity in profiling process.

--large-index
Bowtie2 large index mode.

--len-filt
Whether to filter alignment results by alignment length in profiling process.

--local
If set, input/output file paths are treated as local file system. By default, input/output file paths are treated as HDFS.

--min-identity
The minimal alignment identity of reserved sequence. Default: 0.8

--min-align-len
The minimal alignment length of reserved sequence. Default: 30

--mpa-marker-list
Marker information list for "--prof-pipe meph", extracted from MetaPhlAn2 database mpa_v20_m200.pkl. The file is in json format. User may generate the file using python3 json.dump(mpa_pkl["markers"]).

--mpa-exclude-list
Markers to exclude for "--prof-pipe meph", one marker gene per line. Please reference to variable markers_to_exclude/ingnore_markers in MetaPhlAn2 source code for more information.

--mpa-taxon-list
Taxonomy information list for "--prof-pipe meph", extracted from MetaPhlAn2 database mpa_v20_m200.pkl. The file is in json format. User may generate the file using python3 json.dump(mpa_pkl["taxonomy"]).

-n, --partition-per-sam
Partition number of each sample (for alignment process only, thus we recommend using "--npart-align"). The real partition number for Spark partitioner is (sampleNumber * partition-per-sam). For example, if you have 10 samples and set this para as 5, the RDD will be split to 50 partitions. Increase the number properly may improve the performance. Default: 10

-o, --output-hdfs-dir
Path to store alignment and profiling results, support both local path (file://) and HDFS (Hadoop Distributed File System) path. Note that the "alignment" and "profiling" subdirectory will be created.

--prof-executor-cores
Executor core number in profiling process. The config in Spark is spark.executor.cores/--executor-cores , we make it more flexible. Default: 1".

--prof-executor-memory
Executor memory in profiling process. The value should be at least 512m or users may encounter some exception of memory. Example: 512m 1g. Default: 512m".

--prof-executor-number
Executor number in profiling process. Users may choose the value according to availible resource. Default: 3.

--prof-pipe
Pipeline of profiling. Option: comg (doi:10.1038/nbt.2942), meph (MetaPhlAn2). Default: comg.

--prof-task-cpus
Task cpu (core) number in profiling process. The config in Spark is spark.task.cpu , we make it more flexible.Default: 1.

-r, --ref-matrix
Reference information matrix file of marker gene. Including sequence length, species information of marker gene. File format (tab delimited):
geneID geneName geneLength geneGC species [genus phylum] (header line not included)
1 T2D-6A_GL0083352 88230 s__unclassed [geneGC [g__unclassed p__unclassed]]
...
59 585054.EFER_0542 21669 s__Escherichia_coli [geneGC [g__Escherichia p__Proteobacteria]]
...

-s, --multi-sam-list
Multiple-sample SAM file list, Note that ReadGroupID info is not considered in current version. the option is exclusive to "-i". File format (tab delimited):
ReadGroupID1 sample(SMTag)1 /path/to/rg1_part1.sam
ReadGroupID1 sample(SMTag)1 /path/to/rg1_part2.sam
ReadGroupID2 sample(SMTag)2 /path/to/rg2_part1.sam
...

--skip-alignment
Switch option. If set, the alignment process will be skipped, and users must provide formatted SAM sample list (argument "-s").

--skip-profiling
Switch option. If set, the profiling process will be skipped, and users must provide formatted FASTQ sample list (argument "-i").

--driver-tmp-dir
Local temp directory on driver node. The directory is used for storing files generated by non-spark codes. Default is /tmp/SOAPMetaS_Driver_temp . The temp directory is used to save SAM sample list file..

-x, --index
The alignment index file prefix, utilized by bowtie2. Refer to Bowtie2 manual (http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml) for more information.
```

## 3. Usage

### 3.1 Simple command

All options exist in these two commands are either necessary or highly-recommended.

```bash
# --prof-pipe comg
spark-submit --master <URL> --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores 1 --conf spark.task.cpus=1 --class org.bgi.flexlab.metas.SOAPMetas SOAPMetaS.jar --prof-pipe comg -i sample.list -x /path/to/Bowtie2ReferenceIndex_prefix --large-index -o /path/to/results -n 3 -e "--very-sensitive --phred33 --no-unal --xeq --threads 1" --driver-tmp-dir /path/to/local/temp --ana-lev species --ref-matrix marker_gene_info.matrix --spe-gc reference_genome_info.list 1>running.o 2>running.e

# --prof-pipe meph
spark-submit --master <URL> --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores 1 --conf spark.task.cpus=1 --class org.bgi.flexlab.metas.SOAPMetas SOAPMetaS.jar --prof-pipe meph -i sample.list -x /path/to/Bowtie2ReferenceIndex_prefix -o /path/to/results -n 3 -e "--very-sensitive --phred33 --no-unal --xeq --threads 1" --driver-tmp-dir /path/to/local_temp --ref-matrix marker_gene_info.matrix --mpa-marker-list mpa_marker_list.json --mpa-taxon-list mpa_taxonomy_list.json --mpa-exclude-list MetaPhlAn2_excluded.list 1>running.o 2>running.e
```

### 3.2 IMPORTANT NOTES BEFORE RUNNING

+ **All reference dataset should be deployed to the same absolute path on each worker nodes, or be stored in a public sharing node.**
+ **During our test, we found that Spark executor doesn't support two or more simultaneous call of Bowtie2 JNI. This means that options `--executor-cores` (`--conf spark.executor.cores`) and `--conf spark.task.cpus` configuration must be equal to avoid memory exception in alignment process. However, these two options will also affect the performance of profiling process, we thus provide seven options to control the allocation of resource in two processes seperately, they are `align-executor-memory`,`align-executor-number`,`align-executor-cores`,`prof-executor-memory`,`prof-executor-number`,`prof-executor-cores` and `prof-task-cpus`. The JavaSparkContext will be stopped and recreated before profiling process.**
+ Users can submit SOAPMetaS to Spark Standalone or YARN cluster manager, and change `--master` to correponding address.
+ It's better to set `--deploy-mode` to "client", since we haven't tested "cluster" mode.
+ `--driver-memory` should be set according to the reference matrix file (`--ref-matrix`), it must cover both reference name and other memory needs. The IGC reference matrix for `--prof-pipe comg` needs around 700MB memory (4GB is better), and metaphlanDB for `--prof-pipe meph` needs around 80MB memory (2GB is better).
+ The `--executor-memory` must be set according to the file size of reference indexes (`-x`), it must cover both index contents and other memory needs. The IGC reference for `--prof-pipe comg` needs around 20GB memory per executor (25GB is better), and metaphlanDB for `--prof-pipe meph` needs around 2GB memory per executor (3GB is better).
+ Output directory (-o) "/path/to/results" can be HDFS or local (with `--local` option) path.

### 3.3 Advanced usage

As we explain in section 3.2, seven options are used for resource scheduling: `align-executor-memory`,`align-executor-number`,`align-executor-cores`,`prof-executor-memory`,`prof-executor-number`,`prof-executor-cores` and `prof-task-cpus`. All of these options can affect the performance of SOAPMetaS, besides, `partition-per-sam` option also has effect on performance. In order to limit the executor number, executor memory and total cores allocated to SOAPMetaS , users should set the value of these option carefully.

<br/>

For example, we have 96g memory and 32 cores for SOAPMetaS for 16 samples, we could use the following combination:

  ```text
  --partition-per-sam 2
  --align-executor-memory 3g
  --align-executor-number 32
  --align-executor-cores 1
  --prof-executor-memory 6g
  --prof-executor-number 16
  --prof-executor-cores 2
  --prof-task-cpus 1
  ```

This combination could maximize the resource utilization ratio ideally (but doesn't necessarily mean the best and most speedy).

<br/>

If users decide to use these seven options, similar configurations of `spark-submit` (passed by its commandline option or `--conf`) should be discarded, otherwise they will not take effect. Here is the table of corresponding options and configurations:

|SOAPMetaS Options|Spark Configurations|`spark-submit` Options|
|-------|--------|-------|
|`--align-executor-memory`|`spark.executor.memory`|`--executor-memory`|
|`--align-executor-number`|`spark.executor.instances`|`--num-executors`|
|`--align-executor-cores`|`spark.executor.cores`|`--executor-cores`|
|`--prof-executor-memory`|`spark.executor.memory`|`--executor-memory`|
|`--prof-executor-number`|`spark.executor.instances`|`--num-executors`|
|`--prof-executor-cores`|`spark.executor.cores`|`--executor-cores`|
|`--prof-task-cpus`|`spark.task.cpus`|-|

## 4. Examples

**1) Environment:**

+ CentOS, Ubuntu or Manjaro
+ Java 1.8
+ Spark 2.4.4 with hadoop 2.7 ([download link](https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz))

<br/>

We have tested SOAPMetaS on Ubuntu 18.04.4 LTS (2 processors, 4096 MB base memory) in Virtualbox (MacOS), 5.4.39-1-MANJARO on PC (4 Intel(R) Core(TM) i5-4590 CPU @ 3.30GHz, 8GB memory), and CentOS 3.10.0-514.el7.x86_64 on our server.

**2) Start Standalone:**

```bash
cd spark-2.4.4-bin-hadoop2.7/

echo "export SPARK_MASTER_OPTS=\"-Dspark.deploy.defaultCores=4\""  >> conf/spark-env.sh
echo "export SPARK_WORKER_CORES=2" >> conf/spark-env.sh
echo "export SPARK_WORKER_MEMORY=2g" >> conf/spark-env.sh
echo "export SPARK_WORKER_INSTANCES=2" >> conf/spark-env.sh

bash sbin/start-master.sh
bash sbin/start-slave.sh
```

**3) Run Example:**

+ step 1: Download the `example/profiling_without_HDFS` folder in SOAPMetaS github repository.
+ step 2: Modify `profiling_without_HDFS_example.sh` script in `example/profiling_without_HDFS`. Users should change the file location and paths, and change master URL to actual spark master address.
+ step 3: Run shell script in folder `example/profiling_without_HDFS`.

**More examples scripts can be found in `example` folder in this repository.**

## 5. Build from Source

### 5.1 Build Bowtie2 Native Library

#### 1) Bowtie2 library prerequisites

+ Bowtie2 source code >=2.3.5
+ libtbb (Install through package manager. Please follow [Bowtie2 manual](http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml#building-from-source) for more information)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h)

#### 2) Building Bowtie2 library

+ step 1: Download Bowtie2 source code archive file, we name it as `bowtie2-source.zip`.
+ step 2: Download all `org_bgi_flexlab_metas_alignment_metasbowtie2_Bowtie*` file to the same folder.
+ step 3: Run commands:

  ```Bash
  unzip bowtie2-source.zip

  # compile bowties_jni
  g++ -c -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98   -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB  -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\""  -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM  -DBOWTIE2 -DNDEBUG -Wall -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux   org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp -o bowties_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy  -fPIC -shared

  # compile bowtiel_jni
  g++ -c -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98   -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB  -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\""  -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM  -DBOWTIE2 -DBOWTIE_64BIT_INDEX -DNDEBUG -Wall -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux  org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp -o bowtiel_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy   -fPIC -shared

  cd bowtie2-source

  # compiling and linking of libbowtie_s.so
  g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98  -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB   -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBOWTIE2_VERSION="\"`cat VERSION`\""   -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\""  -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DNDEBUG -Wall -I third_party -I$ {JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o ../libbowties.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.  cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp  random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp  aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp   dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp  aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp   aligner_swsse_ee_u8.cpp aligner_driver.cpp ../bowties_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

  # compiling and linking of libbowtie_l.so
  g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98  -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB   -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBOWTIE2_VERSION="\"`cat VERSION`\""   -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\""  -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DBOWTIE_64BIT_INDEX -DNDEBUG -Wall -I  third_party -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o ../libbowtiel.so bt2_search.cpp ccnt_lut.cpp  ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.  cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp   aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.  cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp  sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp  aligner_swsse_ee_u8.cpp aligner_driver.cpp ../bowtiel_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

  cd ../
  ```

The output `libbowties.so` and `libbowtiel.so` files are Bowtie2 native libraries for "short" and "large" index respectively, and these two files should be move to `main/native` before packing SOAPMetaS.

### 5.2 Generate SOAPMetaS Jar Package

Make sure that `libbowtiel.so`, `libbowties.so`, `libtbb.so.2`, `libtbbmalloc.so.2` and `libtbbmalloc_proxy.so.2` files are in `main/native` folder. `libtbb*` files are copied from C/C++ library "libtbb", `libbowtie*` files are generated in section 4.1 . We provide all these files in `main/native` folder of our repository. And finally, in project root directory, run:

```Bash
mvn package
```

The command will compile and pack all files into a single `.jar` file named "SOAPMetas-*-jar-with-dependencies.jar" in `./target/` folder.

## 6. Known Issues

+ The version of `libtbb` and `libstdc++` should match the version of Bowtie2, and it's recommended that the reference index files are generated using `bowtie2-build` of the same version.
+ `-std=c++98` option works well when compile bowtie2-2.3.5 .
+ Stage-level resource scheduling method is not supported in current version of Spark, we thus "recreate JavaSparkContext" to adjust resourc configuration, and in consequence, one SOAPMetaS will create two Spark applications for alignment and profiling.
+ Multiple alignment tasks in one Spark executor will cause memory-related exception. So the `spark.executor.cores` and `spark.task.cpus` configurations must be the equal (we've mentioned in section 3.2), and this is the reason we didn't provide `align-task-cpus` option.

## 7. License

This project is licensed under the GPLv3 License, see the [LICENSE](LICENSE) file for details.

## 8. Acknowledgments
