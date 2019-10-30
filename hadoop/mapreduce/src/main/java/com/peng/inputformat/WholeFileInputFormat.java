package com.peng.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    /**
     * 创建RecordReader
     * @param split 文件
     * @param context
     * @return RecordReader
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<NullWritable, BytesWritable> reader = new WholeRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
