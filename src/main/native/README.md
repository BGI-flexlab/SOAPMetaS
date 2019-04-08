# JNI script compilation

## Test on hadoop server

Here is the first "semi-successful" compiling command.

```shell
g++ -fPIC -DWITH_TBBQ -DBOWTIE2_VERSION="\"2.3.4.1\"" -DBUILD_HOST="\"cngb-hadoop-a17-7.cngb.sz.hpc\"" -DBUILD_TIME="\"Fri Mar  1 15:45:53 HKT 2019\"" -DCOMPILER_VERSION="\"gcc version 4.8.5 (GCC)\"" -DCOMPILER_OPTIONS="\"-DWITH_TBBQ -std=c++11 -O3 -m64 -msse2 -funroll-loops -g3\"" -std=c++11 -O3 -m64 -msse2 -funroll-loops -g3 -Wno-deprecated -Wall -DBOWTIE2 -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I"/hwfssz1/BIGDATA_COMPUTING/heshixu/TestDir/NativeTest/main/native/bowtie2-source" -shared -o libbowtie.so org_bgi_flexlab_test_BowtieJNI.cpp
```

Here I include the ```bt2_search.cpp``` in the ```org_bgi_flexlab_test_BowtieJNI.cpp``` file, which is strange. With this lib file, the excution of the class invoked an "unreferenced symbol" exception.

Traps during my attempts:

+ The standard compiling command could be obtained by running "make" command (just compiling and linking the original program).
+ Keep in mind that ```extern "C"``` should be placed in the right rows in the right scripts.
+ Pay attention to the compiling order of all the cpp scripts. Wrong order can cause the "undefined reference" during compiling.
+ Do not forget to use ```-shared``` parameter of ```g++```
+ Link to extern "C" function (function in script B) in a c++ script A from another c++ script B, the most important point is the **order of compiling**，wrong order may trigger error like "undefined symbol".

Successful command after these efforts (optimized bowtie2-2.3.4.1 version):

```shell
# For small index
g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -DPOPCNT_CAPABILITY -DWITH_TBB -DWITH_TBBQ -std=c++11\"" -DPOPCNT_CAPABILITY -DWITH_TBB -DWITH_TBBQ -std=c++11 -fno-strict-aliasing -DBOWTIE2_VERSION="\"2.3.4.1\"" -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE  -DBOWTIE_MM  -DBOWTIE2 -DNDEBUG -Wno-deprecated -Wall -I"/hwfssz1/BIGDATA_COMPUTING/heshixu/TestDir/NativeTest/main/native/bowtie2-source/third_party" -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I"/hwfssz1/BIGDATA_COMPUTING/heshixu/TestDir/NativeTest/main/native/bowtie2-source" -o libbowtie_s.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp aligner_swsse_ee_u8.cpp aligner_driver.cpp bowtie_jni.cpp -fPIC -lz -lpthread -ltbb -ltbbmalloc_proxy -shared

# For large index
g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -DPOPCNT_CAPABILITY -DWITH_TBB -DWITH_TBBQ -std=c++11\"" -DPOPCNT_CAPABILITY -DWITH_TBB -DWITH_TBBQ -DBOWTIE_64BIT_INDEX std=c++11 -fno-strict-aliasing -DBOWTIE2_VERSION="\"2.3.4.1\"" -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE  -DBOWTIE_MM  -DBOWTIE2 -DNDEBUG -Wno-deprecated -Wall -I"/hwfssz1/BIGDATA_COMPUTING/heshixu/TestDir/NativeTest/main/native/bowtie2-source/third_party" -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I"/hwfssz1/BIGDATA_COMPUTING/heshixu/TestDir/NativeTest/main/native/bowtie2-source" -o libbowtie_s.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp aligner_swsse_ee_u8.cpp aligner_driver.cpp bowtie_jni.cpp -fPIC -lz -lpthread -ltbb -ltbbmalloc_proxy -shared

# Don't forget to replace "bowtie_jni.cpp" with right JNI file name.
```

## New compilation-linking process for JNI-api with bowtie2-2.3.5 source code

```shell
cd src/main/native

# compiling of bowties_jni
g++ -c -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DNDEBUG -Wall -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.cpp -o bowties_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

# compiling of bowtiel_jni
g++ -c -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DBOWTIE_64BIT_INDEX -DNDEBUG -Wall -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.cpp -o bowtiel_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

cd bowtie2-2.3.5

# compiling-linking of libbowtie_s.so
g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBOWTIE2_VERSION="\"`cat VERSION`\"" -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DNDEBUG -Wall -I third_party -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o ../libbowties.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp aligner_swsse_ee_u8.cpp aligner_driver.cpp ../bowties_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared

# compiling-linking of libbowtie_l.so
g++ -O3 -m64 -msse2 -funroll-loops -g3 -DCOMPILER_OPTIONS="\"-O3 -m64 -msse2 -funroll-loops -g3 -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1\"" -std=c++98 -DPOPCNT_CAPABILITY -DWITH_TBB -DNO_SPINLOCK -DWITH_QUEUELOCK=1 -fno-strict-aliasing -DBOWTIE2_VERSION="\"`cat VERSION`\"" -DBUILD_HOST="\"`hostname`\"" -DBUILD_TIME="\"`date`\"" -DCOMPILER_VERSION="\"`g++ -v 2>&1 | tail -1`\"" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DBOWTIE_MM -DBOWTIE2 -DBOWTIE_64BIT_INDEX -DNDEBUG -Wall -I third_party -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o ../libbowtiel.so bt2_search.cpp ccnt_lut.cpp ref_read.cpp alphabet.cpp shmem.cpp edit.cpp bt2_idx.cpp bt2_io.cpp bt2_util.cpp reference.cpp ds.cpp multikey_qsort.cpp limit.cpp random_source.cpp qual.cpp pat.cpp sam.cpp read_qseq.cpp aligner_seed_policy.cpp aligner_seed.cpp aligner_seed2.cpp aligner_sw.cpp aligner_sw_driver.cpp aligner_cache.cpp aligner_result.cpp ref_coord.cpp mask.cpp pe.cpp aln_sink.cpp dp_framer.cpp scoring.cpp presets.cpp unique.cpp simple_func.cpp random_util.cpp aligner_bt.cpp sse_util.cpp aligner_swsse.cpp outq.cpp aligner_swsse_loc_i16.cpp aligner_swsse_ee_i16.cpp aligner_swsse_loc_u8.cpp aligner_swsse_ee_u8.cpp aligner_driver.cpp ../bowtiel_jni.o -lz -lpthread -ltbb -ltbbmalloc_proxy -fPIC -shared
```

## Note

+ 根据 SparkBwa 的编译方法，可以先对 jni.c(pp) 进行编译不连接，将所有编译得到的 object 放在一个目录下再进行连接。
+ The first string(char[]) argument passed to ``` int main(int argc, char* argv[]) ``` is the name of executor "bowtie2"
+ Copy the two special headers from bowtie_main.cpp into bowtie jni .cpp script, then move the header and .cpp file of bowtie jni into bowtie source code directory, compile using the commands mentioned above to generate the libxxx.so file for small and large index file respectively, finally copy the two .so files to "target" directory. There should be two different JNI wrapper for these two lib files.
+ Remember to add -Djava.library.path=. to java command.

## Reference

<https://www3.ntu.edu.sg/home/ehchua/programming/java/JavaNativeInterface.html>
<https://nachtimwald.com/2017/06/06/wrapping-a-c-library-in-java/>

