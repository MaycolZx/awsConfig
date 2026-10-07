package com.motorbusqueda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.InputStream;
import java.io.IOException;

public class XmlInputFormat extends FileInputFormat<LongWritable, Text> {
    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }

    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[] startTag;
        private byte[] endTag;
        private long start;
        private long end;
        private InputStream in;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private boolean isCompressed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
            endTag = conf.get(END_TAG_KEY).getBytes("utf-8");

            FileSplit fileSplit = (FileSplit) split;
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(file);

            // Detectar si el archivo está comprimido (.bz2)
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
            CompressionCodec codec = codecFactory.getCodec(file);

            if (codec != null) {
                isCompressed = true;
                in = codec.createInputStream(fsin);
                end = Long.MAX_VALUE; // Al descomprimir, leemos hasta agotar el stream
            } else {
                fsin.seek(start);
                in = fsin;
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            long pos = isCompressed ? 0 : fsin.getPos();
            if (pos < end || isCompressed) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(isCompressed ? 0 : fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = in.read();
                if (b == -1) return false;
                if (withinBlock) buffer.write(b);
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else {
                    i = 0;
                }
                if (!withinBlock && i == 0 && !isCompressed && fsin.getPos() >= end) return false;
            }
        }

        @Override
        public LongWritable getCurrentKey() { return key; }

        @Override
        public Text getCurrentValue() { return value; }

        @Override
        public float getProgress() throws IOException {
            if (isCompressed) return 0.5f;
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException { 
            if (in != null) in.close();
            if (fsin != null) fsin.close(); 
        }
    }
}
