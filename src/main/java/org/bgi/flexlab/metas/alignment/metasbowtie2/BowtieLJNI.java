package org.bgi.flexlab.metas.alignment.metasbowtie2;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bgi.flexlab.metas.util.NativeUtils;

import java.io.IOException;

/**
 * ClassName: BowtieLJNI
 * Description: JNI wrapper for bowtie-align-l.
 *
 * @author heshixu@genomics.cn
 */

public class BowtieLJNI {

    protected static final Logger LOG = LogManager.getLogger(BowtieLJNI.class);

    static {
        try{
            NativeUtils.loadLibraryFromJar("/libtbbmalloc.so.2");
            NativeUtils.loadLibraryFromJar("/libtbbmalloc_proxy.so.2");
            NativeUtils.loadLibraryFromJar("/libtbb.so.2");
            NativeUtils.loadLibraryFromJar("/libbowtiel.so");
        } catch (IOException e) {
            LOG.error("[SOAPMetas::" + BowtieLJNI.class.getName() + "] NativeUtils IOException: " + e.toString());
        } catch (NoClassDefFoundError e) {
            LOG.error("[SOAPMetas::" + BowtieLJNI.class.getName() + "] NativeUtils triggered error while loading library: " + e.toString());
        } catch (Exception e){
            LOG.error("[SOAPMetas::" + BowtieLJNI.class.getName() + "] NativeUtils triggered more Exceptions: " + e.toString());
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
    public int bowtieJNI(String[] args, String log){

        int returnCode = this.bowtie_jni(args.length, args, log);

        return returnCode;
    }

    private native int bowtie_jni(int argc, String[] argv, String log);
}
