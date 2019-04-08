#include <iostream>
#include <fstream>
#include <string.h>
#include <stdlib.h>
#include <jni.h>

#include "org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI.h"

using namespace std;

extern "C" {
    int bowtie(int argc, const char** argv);
}

JNIEXPORT jint JNICALL Java_org_bgi_flexlab_metas_alignment_metasbowtie2_BowtieSJNI_bowtie_1jni (JNIEnv *env, jobject obj, jint argc, jobjectArray argStrArray){
    
    const char** argv;

    const char* argvTemp[argc];

    int argsCount = env->GetArrayLength(argStrArray);

    if ( argsCount != argc ){
        fprintf(stderr, "[BowtieJNI C++] Arguments ArrayLength (%d) unequal to argc (%d).\n", argsCount, argc);
        return -1;
    }
    
    for(int i=0; i<argsCount; i++){
        
        jstring argStr = (jstring) env -> GetObjectArrayElement(argStrArray, i);

        argvTemp[i] = env -> GetStringUTFChars(argStr, 0);
    }

    argv = argvTemp;

    return bowtie(argc, argv);

}