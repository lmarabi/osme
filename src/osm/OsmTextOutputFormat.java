/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package osm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import osm.OsmExtractor.MapClass.fileTag;

/**
 *
 * @author turtle
 */
public class OsmTextOutputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = job.get("mapred.textoutputformat.separator",
                "\t");
        if (!isCompressed) {
            Path file = FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem fs = file.getFileSystem(job);
            return new OsmTextOutputFormat.LineRecordWriter<K, V>(file, fs, progress, keyValueSeparator);
        } else {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(job, GzipCodec.class);
            // create the named codec
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
            // build the filename including the extension
            Path file =
                    FileOutputFormat.getTaskOutputPath(job,
                    name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new OsmTextOutputFormat.LineRecordWriter<K, V>(file, fs, progress,
                    keyValueSeparator);
        }
    }

    protected static class LineRecordWriter<K, V>
            implements RecordWriter<K, V> {

        private static final String utf8 = "UTF-8";
        private static final byte[] newline;

        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }
        //protected DataOutputStream out;
        private FileSystem fs;
        private Progressable progress;
        private Path file;
        private final byte[] keyValueSeparator;
        private DataOutputStream[] csvFile = new DataOutputStream[fileTag.values().length];
        private OsmExtractor.MapClass.fileTag csvtag;

        public LineRecordWriter(Path path, FileSystem fs, Progressable progress, String keyValueSeparator) throws IOException {
            this.file = path;
            this.fs = fs;
            this.progress = progress;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

//        public LineRecordWriter(DataOutputStream out) {
//            this(out, "\t");
//        }
        /**
         * Write the object to the byte stream, handling Text as a special case.
         *
         * @param o the object to print
         * @throws IOException if the write throws, we pass it on
         */
        private void writeObject(K key,Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                csvFile[Integer.parseInt(key.toString())].write(to.getBytes(), 0, to.getLength());
            } else {
                csvFile[Integer.parseInt(key.toString())].write(o.toString().getBytes(utf8));
            }
        }

//        private void setOutputStream(String key) throws IOException {
//            csvtag = fileTag.enumOf(Integer.parseInt(key));
//            Path csv = this.file.suffix("_" + csvtag.toString() + ".csv");
//            if (csvFile[csvtag.ordinal()] == null) {
//                csvFile[csvtag.ordinal()] = this.fs.create(csv, this.progress);
//                this.out = csvFile[csvtag.ordinal()];
//            } else {
//                this.out = csvFile[csvtag.ordinal()];
//            }
//        }

        public synchronized void write(K key, V value)
                throws IOException {
            if(csvFile[Integer.parseInt(key.toString())] == null){
                csvtag = fileTag.enumOf(Integer.parseInt(key.toString()));
                Path csv = this.file.suffix("_" + csvtag.toString() + ".csv");
                csvFile[csvtag.ordinal()] = this.fs.create(csv, this.progress);
            }
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                //writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                //out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(key,value);
            }
            
            csvFile[Integer.parseInt(key.toString())].write(newline);
        }

        public synchronized void close(Reporter reporter) throws IOException {
            for (DataOutputStream csvStream : csvFile) {
                if (csvStream != null) {
                    csvStream.close();
                }
            }
        }
    }//end LineRecordWriter class
}//end OsmTextOutputFormat class
