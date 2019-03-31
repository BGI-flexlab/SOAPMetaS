package org.bgi.flexlab.metas.alignment.metasbowtie2;

import cz.adamh.utils.NativeUtils;

import java.io.IOException;

/**
 * ClassName: BowtieLJNI
 * Description: JNI wrapper for bowtie-align-l.
 *
 * @author heshixu@genomics.cn
 */

public class BowtieLJNI {

    static {
        try{
            NativeUtils.loadLibraryFromJar("/libbowtiel.so");
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     *
     * @param args Arguments of bowtie aligner.
     * @return Return code of bowtie() function in native script.
     *
     * TODO: 添加处理bowtie的 --al- 和 --un- 开头的输出控制参数。
     * TODO: 后续考虑是否需要添加 std err 的输出文件
     */
    public int bowtieJNI(String[] args){

        int returnCode = this.bowtie_jni(args.length, args);

        return returnCode;
    }

    private native int bowtie_jni(int argc, String[] argv);
}
