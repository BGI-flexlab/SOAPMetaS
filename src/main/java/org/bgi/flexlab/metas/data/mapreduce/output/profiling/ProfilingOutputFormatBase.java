package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * ClassName: ProfilingOutputFormatBase
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public abstract class ProfilingOutputFormatBase<K, V> extends MultipleOutputFormat<K, V> {

    private TextOutputFormat<K, V> theTextOutputFormat = null;

    @Override
    /**
     * Create a composite record writer that can write key/value data to different
     * output files
     *
     * @param fs
     *          the file system to use
     * @param job
     *          the job conf for the job
     * @param name
     *          the leaf file name for the output file (such as part-00000")
     * @param arg3
     *          a progressable for reporting progress.
     * @return a composite record writer
     * @throws IOException
     */
    public RecordWriter<K, V> getRecordWriter(FileSystem fs, JobConf job,
                                              String name, Progressable arg3) throws IOException {

        final FileSystem myFS = fs;
        final String myName = generateLeafFileName(name);
        final JobConf myJob = job;
        final Progressable myProgressable = arg3;
        final String outHDFSDir = job.get("metas.profiling.outputHdfsDir");
        final String appName = job.get("metas.application.name");

        return new RecordWriter<K, V>() {

            // a cache storing the record writers for different output files.
            TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<String, RecordWriter<K, V>>();

            public void write(K key, V value) throws IOException {

                // get the file name based on the key
                // String keyBasedPath = generateFileNameFromKeyValue(key, value, myName);
                String keyBasedPath = generateFileNameFromKeyValue(key ,value, appName);

                // get the file name based on the input file name
                // String finalPath = getInputFileBasedOutputFileName(myJob, keyBasedPath);
                String finalPath = outHDFSDir + '/' + keyBasedPath;

                // get the actual key
                K actualKey = generateActualKey(key, value);
                V actualValue = generateActualValue(key, value);

                RecordWriter<K, V> rw = this.recordWriters.get(finalPath);
                if (rw == null) {
                    // if we don't have the record writer yet for the final path, create
                    // one
                    // and add it to the cache
                    rw = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
                    this.recordWriters.put(finalPath, rw);
                }
                rw.write(actualKey, actualValue);
            }

            public void close(Reporter reporter) throws IOException {
                Iterator<String> keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter<K, V> rw = this.recordWriters.get(keys.next());
                    rw.close(reporter);
                }
                this.recordWriters.clear();
            }
        };
    }

    abstract protected String generateFileNameFromKeyValue(K key, V value, String appName);

    @Override
    protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs, JobConf job,
                                                     String name, Progressable arg3) throws IOException {
        if (theTextOutputFormat == null) {
            theTextOutputFormat = new TextOutputFormat<K, V>();
        }
        return theTextOutputFormat.getRecordWriter(fs, job, name, arg3);
    }
}
