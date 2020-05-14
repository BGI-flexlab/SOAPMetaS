# SOAPMetaS

An Apache Spark<sup>TM</sup> based tool for profiling large metagenome datasets accurately on distributed cluster.

## Prerequisites

+ Necessary
  + Java (1.8 tested)
  + Spark (2.4.4 tested)
  + Hadoop (2.7 tested)

<br/>

+ Optional
  + Maven 3.2.5 (for building from source)
  + YARN latest (for distributed system)
  + HDFS latest (for distributed file storage)

## Important Parameters

```Text
--ana-lev
The cluster level of output profiling result, including "marker" level for gene profiling and "species" level for species profiling. The parameter work with "--prof-pipe comg". Default: species.

-e, --extra-arg
Other parameters for Bowtie2. Please refer to Bowtie2 manual (http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml) for more information. All parameters should be enclosed together with quotation marks "". Default: "--very-sensitive --no-unal".

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
Partition number of each sample. The real partition number for Spark partitioner is (sampleNumber * partition-per-sam). For example, if you have 10 samples and set this para as 5, the RDD will be split to 50 partitions. Increase the number properly may improve the performance. Default: 10

-o, --output-hdfs-dir
Path to store alignment and profiling results, support both local path (file://) and HDFS (Hadoop Distributed File System) path. Note that the "alignment" and "profiling" subdirectory will be created.

--prof-pipe
Pipeline of profiling. Option: comg (doi:10.1038/nbt.2942), meph (MetaPhlAn2). Default: comg.

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

--tmp-local-dir
Local temp directory for intermediate files. Default is spark.local.dir or hadoop.tmp.dir, or /tmp/ if none is set. The temp directory is used to save alignment results and sample list file.

-x, --index
The alignment index file prefix, utilized by bowtie2. Refer to Bowtie2 manual (http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml) for more information.

```

## Database

### "comg" mode

Including three (types of) files:

+ reference matrix
+ species genome gc list
+ reference sequence index for Bowtie2

File demos are listed in `example/database/marker_data` folder, refer to related [README](https://github.com/BGI-flexlab/SOAPMetaS/tree/dev_maple/example/database/marker_data/README.md) for more information.

### "meph" mode

Including five (types of) files:

+ reference matrix
+ mpa markers json file
+ mpa taxonomy json file
+ MetaPhlAn2 excluded marker list
+ MetaPhlAn2 markers reference sequence index for Bowtie2

File demos are listed in `example/database/metaphlanDB` folder, refer to related [README](https://github.com/BGI-flexlab/SOAPMetaS/tree/dev_maple/example/database/metaphlanDB/README.md) for more information.

## Usage

```bash
# --prof-pipe comg
spark-submit --master <URL> --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores 1 --conf spark.task.cpus=1 --class org.bgi.flexlab.metas.SOAPMetas SOAPMetaS.jar --prof-pipe comg -i sample.list -x /path/to/Bowtie2ReferenceIndex_prefix --large-index -o /path/to/results -n 3 -e "--very-sensitive --phred33 --no-unal --xeq --threads 1" --tmp-local-dir /path/to/local/temp --ana-lev species --ref-matrix marker_gene_info.matrix --spe-gc reference_genome_info.list 1>running.o 2>running.e

# --prof-pipe meph
spark-submit --master <URL> --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores 1 --conf spark.task.cpus=1 --class org.bgi.flexlab.metas.SOAPMetas SOAPMetaS.jar --prof-pipe meph -i sample.list -x /path/to/Bowtie2ReferenceIndex_prefix -o /path/to/results -n 3 -e "--very-sensitive --phred33 --no-unal --xeq --threads 1" --tmp-local-dir /path/to/local_temp --ref-matrix marker_gene_info.matrix --mpa-marker-list mpa_marker_list.json --mpa-taxon-list mpa_taxonomy_list.json --mpa-exclude-list MetaPhlAn2_excluded.list 1>running.o 2>running.e
```

Important Notes:

+ Users can submit SOAPMetaS to Spark Standalone or YARN cluster manager, and change `--master` to correponding address.
+ It's better to set `--deploy-mode` to "client", since we haven't tested "cluster" mode.
+ `--driver-memory` should be set according to the reference matrix file (`--ref-matrix`), it must cover both reference name and other memory needs. The IGC reference matrix for `--prof-pipe comg` needs around 700MB memory (4GB is better), and metaphlanDB for `--prof-pipe meph` needs around 80MB memory (2GB is better).
+ The `--executor-memory` must be set according to the file size of reference indexes (`-x`), it must cover both index contents and other memory needs. The IGC reference for `--prof-pipe comg` needs around 20GB memory per task (25GB is better), and metaphlanDB for `--prof-pipe meph` needs around 2GB memory per task (3GB is better).
+ **All reference dataset should be deployed to the same absolute path on each worker nodes, or be stored in a public sharing node.**
+ Output directory (-o) "/path/to/results" can be HDFS or local (with `--local` option) path.
+ `--executor-cores`, `--conf spark.task.cpus` can affect the performance of SOAPMetaS. The best configuration is related to the volume of data.

## Small Example

**Environment:**

+ Ubuntu 18.04.4 LTS (2 processors, 4096 MB base memory) in Virtualbox
+ Java 1.8
+ Spark 2.4.4 with hadoop 2.7

**Start Standalone:**

```bash
cd spark-2.4.4-bin-hadoop2.7/

echo "export SPARK_MASTER_OPTS=\"-Dspark.deploy.defaultCores=2\""  >> conf/spark-env.sh
echo "export SPARK_WORKER_CORES=1" >> conf/spark-env.sh
echo "export SPARK_WORKER_MEMORY=1g" >> conf/spark-env.sh
echo "export SPARK_WORKER_INSTANCES=2" >> conf/spark-env.sh

bash sbin/start-master.sh
bash sbin/start-slave.sh
```

**Run Example:**

```Bash
## Download the example/profiling_without_HDFS folder of this repository.
## `--prof-pipe meph` requires larger memory then our local-test machine, we thus have no small example for it.

spark-submit --master spark://hostname:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --executor-cores 1 --conf spark.task.cpus=1 --conf spark.dynamicAllocation.enabled=false --class org.bgi.flexlab.metas.SOAPMetas SOAPMetas.jar --local --prof-pipe comg -i example/profiling_without_HDFS/sample.list -x example/profiling_without_HDFS/marker_data/reference_genome -o example/profiling_without_HDFS/results -n 2 -e "--very-sensitive --phred33 --no-unal --xeq --threads 1" --ana-lev species --ref-matrix example/profiling_without_HDFS/marker_data/reference_info.matrix --spe-gc example/profiling_without_HDFS/marker_data/reference_genome_gc.list --tmp-local-dir example/profiling_without_HDFS/temp 1>example/profiling_without_HDFS/running.o 2>example/profiling_without_HDFS/running.e
```

## More Examples

Examples can be found in `example` folder.

## Build from Source

### Build Bowtie2 Native Library

#### Bowtie2 library prerequisites

+ Bowtie2 source code >=2.3.5
+ libtbb (Install through package manager. Please follow [Bowtie2 manual](http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml#building-from-source) for more information)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h)

#### Building Bowtie2 library

1. Download Bowtie2 source code archive file, we name it as `bowtie2-source.zip`.
2. Download all `org_bgi_flexlab_metas_alignment_metasbowtie2_Bowtie*` file to the same folder.

+ Note that `-std=c++98` works for bowtie2-2.3.5.

```Bash
unzip bowtie2-source.zip

# compile bowties_jni
g++ -c -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DNDEBUG -Wall -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp -o bowties_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

# compile bowtiel_jni
g++ -c -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DBOWTIE_64BIT_INDEX -DNDEBUG -Wall -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp -o bowtiel_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

cd bowtie2-source

# compiling and linking of libbowtie_s.so
g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBOWTIE2_VERSION="\"`cat VERSION`\"" -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DNDEBUG -Wall -I third_party -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o ../libbowties.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp aligner_swsse_ee_u8.cpp aligner_driver.cpp ../bowties_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

# compiling and linking of libbowtie_l.so
g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBOWTIE2_VERSION="\"`cat VERSION`\"" -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DBOWTIE_64BIT_INDEX -DNDEBUG -Wall -I third_party -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o ../libbowtiel.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp aligner_swsse_ee_u8.cpp aligner_driver.cpp ../bowtiel_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

cd ../
```

The output `libbowties.so` and `libbowtiel.so` files are Bowtie2 native libraries for "short" and "large" index respectively, and these two files should be move to `main/native` before packing SOAPMetaS.

### Generate SOAPMetaS Jar Package

Make sure that `libbowtiel.so`, `libbowties.so`, `libtbb.so.2`, `libtbbmalloc.so.2` and `libtbbmalloc_proxy.so.2` files are in `main/native` folder. `libbowtie*` can be generated following [these steps](#Build-Bowtie2-Native-Library). `libtbb*` files are copied from C/C++ library "libtbb".

These files have been put in right folder in this repository.

In root directory of the maven project, run:

```Bash
mvn package
```

The command will compile and pack all files into a single `.jar` file named "SOAPMetas-*-jar-with-dependencies.jar" in `./target/` folder.

## Known Issues

1. In Spark `local` mode, SOAPMetaS' multiple partitions (`-n` >1 or more than one input file) will trigger execption related to native libraries, and the cause of the exception hasn't been located yet. But `standalone` and `yarn` mode works well.
2. The version of `libtbb` and `libstdc++` should match the version of Bowtie2, and it's recommended that the reference index files are generated using `bowtie2-build` of the same version.

## License

This project is licensed under the GPLv3 License, see the [LICENSE](LICENSE) file for details.

## Acknowledgments
