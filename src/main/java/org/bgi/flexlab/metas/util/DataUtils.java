package org.bgi.flexlab.metas.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: DataUtils
 * Description:
 *
 * @author heshixu@genomics.cn
 */

public class DataUtils {

    public static int fileCount(JavaSparkContext jsc, String filePath) {
        int count = 0;
        //FileSystem fs;
        try {
            Path[] inputPathPatterns = DataUtils.stringToPath(DataUtils.getPathStrings(filePath));
            for (Path pp : inputPathPatterns) {
                count += pp.getFileSystem(jsc.hadoopConfiguration()).globStatus(pp).length;
            }

            //fs = FileSystem.get(jsc.hadoopConfiguration());
            //RemoteIterator<LocatedFileStatus> fileiter = fs.listFiles(new Path(filePath), recur);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static Path[] stringToPath(String[] str) {
        if (str == null) {
            return null;
        }
        Path[] p = new Path[str.length];
        for (int i = 0; i < str.length; i++) {
            p[i] = new Path(str[i]);
        }
        return p;
    }

    public static String[] getPathStrings(String commaSeparatedPaths) {
        int length = commaSeparatedPaths.length();
        int curlyOpen = 0;
        int pathStart = 0;
        boolean globPattern = false;
        List<String> pathStrings = new ArrayList<String>();

        for (int i = 0; i < length; i++) {
            char ch = commaSeparatedPaths.charAt(i);
            switch (ch) {
                case '{': {
                    curlyOpen++;
                    if (!globPattern) {
                        globPattern = true;
                    }
                    break;
                }
                case '}': {
                    curlyOpen--;
                    if (curlyOpen == 0 && globPattern) {
                        globPattern = false;
                    }
                    break;
                }
                case ',': {
                    if (!globPattern) {
                        pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
                        pathStart = i + 1;
                    }
                    break;
                }
            }
        }
        pathStrings.add(commaSeparatedPaths.substring(pathStart, length));

        return pathStrings.toArray(new String[0]);
    }

    public static String getTmpDir(JavaSparkContext jsc) {

        String dirName = jsc.getLocalProperty("spark.local.dir");

        if (dirName == null || dirName.equals("null") || dirName.isEmpty()) {
            dirName = jsc.hadoopConfiguration().get("hadoop.tmp.dir");
        }
        if (dirName.startsWith("file:")) {
            dirName = dirName.replaceFirst("file:", "");
        }
        File tmpFileDir = new File(dirName);
        if (!tmpFileDir.isDirectory() || !tmpFileDir.canWrite()) {
            dirName = "/tmp/";
        }

        return dirName;
    }

    public static void createFolder(Configuration hadoopConf, String target) throws IOException {

        // Path variable
        Path outputDir = new Path(target);

        FileSystem fs = outputDir.getFileSystem(hadoopConf);

        // Directory creation
        if (!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
        } else {
            fs.delete(outputDir, true);
            fs.mkdirs(outputDir);
        }

    }
}