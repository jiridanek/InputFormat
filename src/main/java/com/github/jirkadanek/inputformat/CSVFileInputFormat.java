package com.github.jirkadanek.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.List;

public class CSVFileInputFormat implements InputFormat<LongWritable, Text> {
    com.github.jirkadanek.inputformat.mapreduce.CSVFileInputFormat delegate;

    CSVFileInputFormat() {
        super();
        this.delegate = new com.github.jirkadanek.inputformat.mapreduce.CSVFileInputFormat();
    }

    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        JobContext context = new JobContextImpl(conf, new JobID());
        List<InputSplit> list = delegate.getSplits(context);
        org.apache.hadoop.mapred.InputSplit[] array = new org.apache.hadoop.mapred.InputSplit[list.size()];
        for (int i = 0; i < list.size(); i++) {
            org.apache.hadoop.mapreduce.lib.input.FileSplit split = (org.apache.hadoop.mapreduce.lib.input.FileSplit) list.get(i);
            array[i] = new FileSplit(split.getPath(), split.getStart(), split.getLength(), split.getLocations());
        }

        return array;
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, final Reporter reporter) throws IOException {
        final FileSplit fileSplit = (FileSplit) split;
        InputSplit inputSplit = new FileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations());
        StatusReporter statusReporter = new StatusReporter() {
            @Override
            public Counter getCounter(Enum<?> name) {
                return reporter.getCounter(name);
            }

            @Override
            public Counter getCounter(String group, String name) {
                return reporter.getCounter(group, name);
            }

            @Override
            public void progress() {
                reporter.progress();
            }

            @Override
            public float getProgress() {
                return reporter.getProgress();
            }

            @Override
            public void setStatus(String status) {
                reporter.setStatus(status);
            }
        };
        TaskAttemptContext context = new TaskAttemptContextImpl(job, new TaskAttemptID(), statusReporter);
        final org.apache.hadoop.mapreduce.RecordReader recordReader = delegate.createRecordReader(inputSplit, context);
        System.out.println("ABOUT TO INITIALIZE");
        try {
            org.apache.hadoop.mapreduce.lib.input.FileSplit mapreduceFileSplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations());
            recordReader.initialize(mapreduceFileSplit, context);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("INITIALIZED");
        return new RecordReader<LongWritable, Text>() {
            @Override
            public boolean next(LongWritable key, Text value) throws IOException {
                boolean ret = false;
                try {
                    ret = recordReader.nextKeyValue();
                    if (ret) {
                        key.set(((LongWritable) recordReader.getCurrentKey()).get());
                        value.set((Text) recordReader.getCurrentValue());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ret;
            }

            @Override
            public LongWritable createKey() {
                return new LongWritable();
            }

            @Override
            public Text createValue() {
                return new Text();
            }

            /**
             * Hack, suffers from floating point arithmetic, but seems to work in practice
             */
            @Override
            public long getPos() throws IOException {
                long start = fileSplit.getStart();
                long end = start + fileSplit.getLength();
                long pos = 0;
                try {
                    pos = (long) (recordReader.getProgress() * (float) (end - start)) + start;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return pos;
            }

            @Override
            public void close() throws IOException {
                recordReader.close();
            }

            @Override
            public float getProgress() throws IOException {
                try {
                    return recordReader.getProgress();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return 1.0f;
            }
        };
    }
}
