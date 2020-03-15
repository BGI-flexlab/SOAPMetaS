package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import htsjdk.samtools.*;
import htsjdk.samtools.util.Locatable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSource;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.disq_bio.disq.impl.formats.sam.SamSource;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

public class MetaSamSource extends SamSource implements Serializable {
    private ValidationStringency validationStringency = ValidationStringency.DEFAULT_STRINGENCY;

    public <T extends Locatable> JavaRDD<SAMRecord> getReads(
            JavaSparkContext jsc,
            String path,
            int splitSize,
            SAMFileHeader samHeader)
            throws IOException {

        // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

        final Configuration conf = jsc.hadoopConfiguration();
        if (splitSize > 0) {
            conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
        }

//        SAMFileHeader samHeader = getFileHeader(jsc, path, validationStringency, referenceSourcePath);
        Broadcast<SAMFileHeader> samHeaderBroadcast = jsc.broadcast(samHeader);

        return textFile(jsc, path)
                .mapPartitions(
                        (FlatMapFunction<Iterator<String>, SAMRecord>)
                                lines -> {
                                    SAMLineParser samLineParser =
                                            new SAMLineParser(
                                                    new DefaultSAMRecordFactory(),
                                                    validationStringency,
                                                    samHeaderBroadcast.getValue(),
                                                    null,
                                                    null);
                                    return stream(lines)
                                            .filter(line -> !line.startsWith("@"))
                                            .map(samLineParser::parseLine)
                                            .filter(samRecord -> !samRecord.getReadUnmappedFlag())
                                            .peek(samRecord -> {samRecord.setBaseQualities(new byte[0]); samRecord.setBaseQualityString("*"); samRecord.setHeaderStrict(null);})
                                            .iterator();
                                });
    }

    private <T extends Locatable> JavaRDD<String> textFile(JavaSparkContext jsc, String path) {
        // Use this over JavaSparkContext#textFile since this allows the configuration to be passed in
        //TODO  读取pair reads
        return jsc.newAPIHadoopFile(
                path, TextInputFormat.class, LongWritable.class, Text.class, jsc.hadoopConfiguration())
                .map(pair -> pair._2.toString())
                .setName(path);
    }
}
