/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package osm;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author turtle
 */
public class OsmRecordReader implements RecordReader<LongWritable, Text> {

    private long start;
    private long end;
    private long pos;
    private FileSystem fileSystem;
    private FSDataInputStream inputStream;
    private LineReader lineReader;
    private Path path;
    private Text tempLine = new Text();
    private CompressionCodecFactory compressionCodecs = null;

    public OsmRecordReader(long start, long end, Path path, Configuration job) {
        this.start = start;
        this.end = end;
        this.pos = start;
        this.path = path;
        try {
            fileSystem = this.path.getFileSystem(job);
            compressionCodecs = new CompressionCodecFactory(job);
            final CompressionCodec codec = compressionCodecs.getCodec(path);
            inputStream = fileSystem.open(this.path);

            // Open the file and start the seek the start of the split
            if(codec != null){
                lineReader = new LineReader(codec.createInputStream(inputStream), job);
                this.end = Long.MAX_VALUE;
            }else{
                inputStream.seek(start);
                lineReader = new LineReader(inputStream);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Call Constructor\nStart:"+start+"\nEnd:"+
                end);
    }

    OsmRecordReader(JobConf job, FileSplit fileSplit) {
        this(fileSplit.getStart(), fileSplit.getStart()+fileSplit.getLength(), fileSplit.getPath(), job);
    }

//    public boolean next(Text text) {
//        boolean flag = false;
//        try {
//            String result = "";
//            text.set(result);
//            int byteRead = 0;
//            while ((byteRead = lineReader.readLine(text)) != 0) {
//                pos += byteRead;
//
//                if (isStartPrimitive(text.toString())) {
//                    result += text.toString() + "\n";
//                    flag = true;
//                    if (isEndTag(text.toString())) {
//                        text.set(result);
//                        return true;
//                    }
//                } else if (flag) {
//                    result += text.toString() + "\n";
//                    if (isEndPrimitive(text.toString())) {
//                        text.set(result);
//                        return true;
//                        
//                    }
//                } else if (!flag && pos > end) {
//                    text.set(result);
//                    return false;
//                }
//
//            }
//
//            text.set(result);
//            return false;
//        } catch (IOException ex) {
//            Logger.getLogger(OsmRecordReader.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return false;
//    }
//    
    private static final byte[] EndXMLTag = "/>".getBytes();
    private boolean isEndTag(Text line){
        return textContains(line, EndXMLTag);
    }

    private static final byte[] StartNode = "<node".getBytes();
    private static final byte[] StartWay = "<way".getBytes();
    private static final byte[] StartRelation = "<relation".getBytes();

    private boolean isStartPrimitive(Text line) {
        return textContains(line, StartNode)
                || textContains(line, StartWay)
                || textContains(line, StartRelation);
    }

    private static final byte[] EndNode = "</node".getBytes();
    private static final byte[] EndWay = "</way".getBytes();
    private static final byte[] EndRelation = "</relation".getBytes();
    
    private boolean isEndPrimitive(Text line) {
        return textContains(line, EndNode)
                || textContains(line, EndWay)
                || textContains(line, EndRelation);
    }
    
    private static boolean textContains(Text text, byte[] str) {
        return textContains(text.getBytes(), text.getLength(), str);
    }
    
    private static boolean textContains(byte[] text, int length1, byte[] str) {
        for (int i = 0; i <= length1 - str.length; i++) {
            int j = 0;
            while (text[i+j] == str[j]) {
                j++;
                if (j == str.length)
                    return true;
            }     
        }
        return false;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        String pathString = System.getProperty("user.dir") + "/Data/real10MB.osm";
        File file = new File(pathString);
        Path path = new Path(pathString);
        long size = 0;
        long start = 0;
        String output = "";
//        OsmRecordReader reader = new OsmRecordReader(file.length()- 6, file.length(), path);
//       Text text = new Text();
//        while(reader.next(new LongWritable(),text)){
//            output += text.toString();
//        }
        long end = 2*1024*1024;
        long blocksize = end;
        Text text = new Text();
        long startTime = System.currentTimeMillis();
        System.out.println("File length: "+file.length());
        try{
        while (size <= file.length()) {
            System.out.println("Reading ["+start+","+end+")");
            OsmRecordReader reader = new OsmRecordReader(start, end, path, new Configuration());
            //LineRecordReader reader = new LineRecordReader(new FileInputStream(pathString), start, end, Integer.MAX_VALUE);
            LongWritable key = reader.createKey();
            while(reader.next(key, text)){
                //output += text.toString()+"\n";
            }
            size += end -start;
            start = (long)end;
            end += blocksize;
        }
        }catch (Exception e){
            long endTime = System.currentTimeMillis();
        System.out.println("Time took in minute :"+((endTime-startTime)*0.001)/60);
        //export(output);
        }
        


    }

    public static void export(String output) {
        File file = new File(System.getProperty("user.dir") + "/Data/result.txt");
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        OutputStreamWriter dos = null;

        try {
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            dos = new OutputStreamWriter(bos, "UTF8");
            file.createNewFile();
            dos.write(output);
            fos.flush();
            bos.flush();
            dos.flush();
            bos.close();
            dos.close();
            fos.close();
            System.out.println("Done creating file");
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
        boolean flag = false;
        try {
            value.clear();
            int byteRead = 0;
            while (((byteRead = lineReader.readLine(tempLine)) != 0 && pos <= end) || flag) {
                pos += byteRead;

                if (isStartPrimitive(tempLine)) {
                    value.append(tempLine.getBytes(), 0, tempLine.getLength());
                    
                    flag = true;
                    if (isEndTag(tempLine)) {
                        key.set(pos);
                        return true;
                    }
                } else if (flag) {
                    value.append(tempLine.getBytes(), 0, tempLine.getLength());
                    if (isEndPrimitive(value)) {
                        return true;
                    }
                } else if (!flag && pos > end) {
                    
                    return false;
                }

            }
            
            return false;
        } catch (IOException ex) {
            Logger.getLogger(OsmRecordReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public void close() throws IOException {
        this.inputStream.close();
    }

    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }
}
