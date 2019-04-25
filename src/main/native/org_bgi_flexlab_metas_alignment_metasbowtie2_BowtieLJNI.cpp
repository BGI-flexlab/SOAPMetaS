#include <iostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>
#include <jni.h>

#include "org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI.h"

using namespace std;

extern "C" {
    int bowtie(int argc, const char** argv);
}

JNIEXPORT jint JNICALL Java_org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieLJNI_bowtie_1jni (JNIEnv *env, jobject obj, jint argc, jobjectArray argStrArray, jstring logFile){
    
    const char** argv;

    const char* argvTemp[argc];

    int argsCount = env->GetArrayLength(argStrArray);

    const char* alnLog = env->GetStringUTFChars(logFile, 0);

    streambuf* backup;
    backup = cerr.rdbuf();

    ofstream logfile;
    logfile.open(alnLog);

    cerr.rdbuf(logfile.rdbuf());

    if ( argsCount != argc ){
        cerr << "[BowtieJNI C++] Arguments ArrayLength (" << argsCount << ") unequal to argc (" << argc << ")." << endl;
        return -1;
    }

    for(int i=0; i<argsCount; i++){
        
        jstring argStr = (jstring) env -> GetObjectArrayElement(argStrArray, i);

        argvTemp[i] = env -> GetStringUTFChars(argStr, 0);
    }

    argv = argvTemp;

    int ret = bowtie(argc, argv);

    cerr.rdbuf(backup);
    logfile.close();

    return ret;
}