package org.bgi.flexlab.metas.data.mapreduce.input.sam;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;


public class HdfsLineReader extends org.apache.hadoop.util.LineReader {

    public HdfsLineReader(InputStream in, Configuration conf)
            throws IOException {
        super(in, conf);
    }
}