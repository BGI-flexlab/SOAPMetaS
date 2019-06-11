package org.bgi.flexlab.metas.data.mapreduce.output.profiling;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * ClassName: MetasAbunOutputFormat
 * Description:
 *
 * @author: heshixu@genomics.cn
 */

public abstract class MetasAbunOutputFormat<K, V> extends FileOutputFormat<K, V> {


    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        String appName = taskAttemptContext.getConfiguration().get("metas.application.name");
        String outputDir = taskAttemptContext.getConfiguration().get("metas.profiling.outputHdfsDir");

        return new RecordWriter<K, V>() {

            // a cache storing the record writers for different output files.
            TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<String, RecordWriter<K, V>>();

            public void write(K key, V value) throws IOException, InterruptedException {

                // get the file name based on the key
                String finalPath = generateFileNameForKeyValue(key, value, outputDir, appName);

                // get the actual key
                K actualKey = generateActualKey(key, value);
                V actualValue = generateActualValue(key, value);

                RecordWriter<K, V> rw = this.recordWriters.get(finalPath);
                if (rw == null) {
                    // if we don't have the record writer yet for the final path, create
                    // one
                    // and add it to the cache
                    rw = getBaseRecordWriter(taskAttemptContext, finalPath);
                    this.recordWriters.put(finalPath, rw);
                }
                rw.write(actualKey, actualValue);
            }

            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                Iterator<String> keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter<K, V> rw = this.recordWriters.get(keys.next());
                    rw.close(taskAttemptContext);
                }
                this.recordWriters.clear();
            }
        };
    }

    /**
     * Generate the file output file name based on the given key and the leaf file
     * name. The default behavior is that the file name does not depend on the
     * key.
     *
     * @param key
     *          the key of the output data
     *
     * @return generated file name
     */
    abstract protected String generateFileNameForKeyValue(K key, V value, String outDir, String appName);

    /**
     * Generate the actual key from the given key/value. The default behavior is that
     * the actual key is equal to the given key
     *
     * @param key
     *          the key of the output data
     * @param value
     *          the value of the output data
     * @return the actual key derived from the given key/value
     */
    protected K generateActualKey(K key, V value) {
        return key;
    }

    /**
     * Generate the actual value from the given key and value. The default behavior is that
     * the actual value is equal to the given value
     *
     * @param key
     *          the key of the output data
     * @param value
     *          the value of the output data
     * @return the actual value derived from the given key/value
     */
    protected V generateActualValue(K key, V value) {
        return value;
    }

    /**
     *
     * @param taskAttemptContext current task context
     *
     * @return A RecordWriter object over the given file
     * @throws IOException
     */
    abstract protected RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext taskAttemptContext, String outputFile) throws IOException, InterruptedException;
}
