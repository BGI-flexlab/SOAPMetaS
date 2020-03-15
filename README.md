# SOAPMetaS

An Apache Spark<sup>TM</sup> based tool for profiling large metagenome datasets accurately on distributed cluster.

## Getting Started

Basic manual.

### Prerequisites

+ Necessary
  + Java >=1.8
  + Spark >=2.4.0
  + Hadoop >=3.0.0

<br>

+ Optional
  + Maven >=3.2.5 (for building from source)
  + YARN latest (for distributed system)
  + HDFS latest (for distributed file storage)

### Database

#### "comg" mode

Including three (types of) files:

+ reference matrix
+ species genome gc list
+ reference sequence index for Bowtie2

File demos are listed in `example/database/marker_data` folder, refer to related [README](https://github.com/BGI-flexlab/SOAPMetaS/tree/dev_maple/example/database/marker_data/README.md) for more information.

#### "meph" mode

Including five (types of) files:

+ reference matrix
+ mpa markers json file
+ mpa taxonomy json file
+ MetaPhlAn2 excluded marker list
+ MetaPhlAn2 markers reference sequence index for Bowtie2

File demos are listed in `example/database/metaphlanDB` folder, refer to related [README](https://github.com/BGI-flexlab/SOAPMetaS/tree/dev_maple/example/database/metaphlanDB/README.md) for more information.

### Usage

#### Basic Parameters

```Text
--ana-lev
The cluster level of output profiling result, including "marker" level for gene profiling and "species" level for species profiling. The parameter work with "--prof-pipe comg".

--prof-pipe
The profiling method, including "comg" and "meph", refer to Supplementary Note 4 for more details.

-x, --index
The alignment index file prefix, utilized by bowtie2.

--large-index
"large index" option of bowtie2.

-e, --extra-arg
Extra parameters for Bowtie2.

-n, --partition-per-sam
Partition number of each sample, increase the number properly may improve the performance.

-i, --multi-sample-list
Input file contains the sample ID, read group ID and file path to FASTQ-format clean reads.

-s, --multi-sam-list
Input file contains the sample ID, read group ID and file path to SAM-format alignment results.

-g, --spe-gc
Genome information (genome length, genome GC) of each species in reference data. The file is used with "--ana-lev species" and "--prof-pipe comg".

--iden-filt
Whether filter alignment results by identity.

--min-identity
The minimal alignment identity of reserved sequence.

--len-filt

Whether filter alignment results by alignment length.

--min-align-len
The minimal alignment length of reserved sequence.

-r, --ref-matrix
Reference information matrix file of marker gene. Including sequence length, species information of marker gene.

-o, --output-hdfs-dir
Path to store alignment and profiling results, support both local path (file://) and HDFS (Hadoop Distributed File System) path.

--tmp-local-dir
Local temp directory to store intermediate files.

--mpa-taxon-list and --mpa-marker-list and --mpa-exclude-list
Reference database for "--prof-pipe meph", all file is excluded from MetaPhlAn2-related files. Refer to Supplementary Note 6.2 for details.

--skip-alignment
Control the execution of alignment step.

--skip-profiling
Control the execution of profiling step.
```

#### Server mode

Server mode is based on distributed system. Users should use YARN (recommended) or other manager (not tested) to start a Spark master.

```Bash
# comg mode
spark-submit --master yarn --class org.bgi.flexlab.metas.SOAPMetas SOAPMetaS.jar org.bgi.flexlab.metas.SOAPMetas --prof-pipe comg -i /path/to/sample.list -x Bowtie2ReferenceIndex_prefix --seq-mode se -o /path/to/output_directory -n 10 -e "--very-sensitive --no-unal --xeq --threads 1" --ana-lev markers --tmp-local-dir /path/to/local_temp --ref-matrix reference.matrix --spe-gc species_genome_gc.list

# meph mode
spark-submit --master yarn --class org.bgi.flexlab.metas.SOAPMetas SOAPMetaS.jar org.bgi.flexlab.metas.SOAPMetas --prof-pipe meph -i /path/to/sample.list -x Bowtie2ReferenceIndex_prefix --seq-mode se -o /path/to/output_directory -n 10 -e "--very-sensitive --no-unal --xeq --threads 1" --ana-lev markers --ref-matrix reference.matrix --tmp-local-dir /path/to/local_temp --mpa-marker-list mpa_marker_list.json --mpa-taxon-list mpa_taxonomy_list.json --mpa-exclude-list MetaPhlAn2_excluded.list
```

#### Local mode

Change argument of `--master` from "yarn" to "local[*]'. Local mode can be executed directly without distributed server.

## Examples

Examples of "comg" and "meph" mode, as well as local mode without HDFS, are in `example` folder.

Note that the executor memory should be set according to the file size of reference indexes. The IGC reference needs around 20GB memory, and metaphlanDB needs around 3GB memory.

## Building from Source

### Generate SOAPMetaS Jar Package

Make sure that `libbowtiel.so`, `libbowties.so`, `libtbb.so.2`, `libtbbmalloc.so.2` and `libtbbmalloc_proxy.so.2` files are in `main/native` folder. `libbowtie*` can be generated following [these steps](#Build-Bowtie2-Native-Library). `libtbb*` files are copied from C/C++ library "libtbb".

These files have been put in right folder in this repository.

In root directory of the maven project, run:

```Bash
mvn package
```

The command will compile and pack all files into a single `.jar` file named "SOAPMetas-*-jar-with-dependencies.jar" in `./target/` folder.

### Build Bowtie2 Native Library

#### Bowtie2 library prerequisites

+ Bowtie2 source code >=2.3.5
+ libtbb (Install through package manager. Please follow [Bowtie2 manual](http://bowtie-bio.sourceforge.net/bowtie2/manual.shtml#building-from-source) for more information)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp)
+ [org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h](https://github.com/BGI-flexlab/SOAPMetaS/blob/dev_maple/src/main/native/org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h)


#### Building Bowtie2 library

Download Bowtie2 source code archive file, we name it as `bowtie2-source.zip`.

Download all `org_bgi_flexlab_metas_alignment_metasbowtie2_Bowtie*` file to the same folder.

Note that `-std=c++98` works for bowtie2-2.3.5.

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

## Known Problems

1. Multiple partitions (`-n` >1 or more than one input file) can cause execption related to native libraries, which might be caused by our local Spark configuration or incompatible native dependencies (such as `libstdc++` or others).
2. The version of `libtbb` and `libstdc++` should match the version of Bowtie2, and it's recommended that the reference index files are generated using `bowtie2-build` of the same version.

## License

This project is licensed under the GPLv3 License, see the [LICENSE](LICENSE) file for details.

## Acknowledgments
