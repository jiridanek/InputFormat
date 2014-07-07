package com.github.jirkadanek.inputformat.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.testng.Assert.assertEquals;


public class CSVRecordReaderTest {
    static String RESOURCE_DIR_PREFIX = "../../../../../";

    int quote = "\"".codePointAt(0);
    int comma = ",".codePointAt(0);

    private Path path;
    private File testFile;
    private InputFormat inputFormat;
    private TaskAttemptContext context;
    private RecordReader reader;
    private long StartOfFirstRecord = 0;
    private long EndOfFirstRecord = 36;
    private long StartOfSecondRecord = 73;
    private long EndOfSecondRecord = 73;
    private long StartOfThirdRecord = 100;
    private long EndOfThirdRecord = 157;

    @BeforeMethod
    public void setUp() throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
        conf.set("fs.default.name", "file:///");

        URI uri = getClass().getResource(RESOURCE_DIR_PREFIX + "teste2.csv").toURI();
        testFile = new File(uri);
        path = new Path(uri);

        inputFormat = ReflectionUtils.newInstance(CSVFileInputFormat.class, conf);
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    private void setUpSplit() throws Exception {
        long start = 0;
        long end = testFile.length();
        setUpSplit(start, end);
    }

//    @Test
//    public void shouldReadFileSplitBetweenRecords() throws Exception {
//        setUpNoSplit();
//
//        while(reader.nextKeyValue()) {
//            System.out.println(reader.getProgress());
//            System.out.println(reader.getCurrentKey());
//            System.out.println(reader.getCurrentValue());
//            System.out.println("---");
//        }
//    }

    private void setUpSplit(long start) throws Exception {
        long end = testFile.length() - start;
        setUpSplit(start, end);
    }

    private void setUpSplit(long start, long end) throws Exception {
        long length = end - start;
        FileSplit split = new FileSplit(path, start, length, null);
        reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);
    }

    @Test
    public void shouldReadForumNodeFile() throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
        conf.set("fs.default.name", "file:///");

        URI uri = getClass().getResource(RESOURCE_DIR_PREFIX + "forum_node.tsv.gz").toURI();
        testFile = new File(uri);
        path = new Path(uri);

        inputFormat = ReflectionUtils.newInstance(CSVFileInputFormat.class, conf);
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        long fileSize = 120313812;

        long start = 0;
        long end = fileSize;
        long length = end - start;
        FileSplit split = new FileSplit(path, start, length, null);
        reader = inputFormat.createRecordReader(split, context);
        reader.initialize(split, context);

        org.apache.hadoop.mapreduce.lib.input.LineRecordReader lineReader = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
        lineReader.initialize(new FileSplit(path, start, testFile.length(), null), context);

        int i = 0;

        while (reader.nextKeyValue() && lineReader.nextKeyValue() /*&& i++ != 10*/) {
            assertEquals(((LongWritable) reader.getCurrentKey()).get(), lineReader.getCurrentKey().get());
            assertEquals(reader.getCurrentValue(), lineReader.getCurrentValue());
//            System.out.println(reader.getProgress());
//            System.out.println(reader.getCurrentKey());
//            System.out.println(reader.getCurrentValue());
            i++;
            if (i == 252206) {
                System.out.println("252207");
            }
        }
        System.out.println(i);
        assertEquals(reader.nextKeyValue(), lineReader.nextKeyValue());
    }

    @Test
    public void shouldReadForumNodeFileStartMiddle() throws Exception {
//        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
//        conf.set("fs.default.name", "file:///");
//
//        URI uri = getClass().getResource("../../forum_node/forum_node.tsv").toURI();
//        testFile = new File(uri);
//        path = new Path(uri);
//
//        URI dirUri = getClass().getResource("../../forum_node").toURI();
//        Path dirPath = new Path(dirUri);
//
//
//        conf.set("mapreduce.input.fileinputformat.inputdir", dirPath.toString());
//
//        inputFormat = ReflectionUtils.newInstance(CSVFileInputFormat.class, conf);
//        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
//
//        List<InputSplit> list = inputFormat.getSplits(context);
//        System.out.println(list.size());
//
//        long fileSize = 120313812;
//
//        long start = 30078696;
//        long end = fileSize;
//        long length = end - start;
//        FileSplit split = new FileSplit(path, start, length, null);
//        reader = inputFormat.createRecordReader(split, context);
//        reader.initialize(split, context);
//
//        org.apache.hadoop.mapreduce.lib.input.LineRecordReader lineReader = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
//        lineReader.initialize(new FileSplit(path, start, testFile.length(), null), context);
//
//        int i = 0;
//        boolean my = true;
//        boolean theirs = true;
//
//        while(reader.nextKeyValue() && lineReader.nextKeyValue() /*&& i++ != 10*/) {
//            assertEquals(((LongWritable) reader.getCurrentKey()).get(), lineReader.getCurrentKey().get());
//            assertEquals(reader.getCurrentValue(), lineReader.getCurrentValue());
////            System.out.println(reader.getProgress());
//            if(i < 10) {
//                System.out.println(reader.getCurrentKey());
//                System.out.println(reader.getCurrentValue());
//            }
//            i++;
//            if (i == 252206) {
//                System.out.println("252207");
//            }
//        }
//        System.out.println(i);
//        assertEquals(reader.nextKeyValue(), lineReader.nextKeyValue());
    }

    @Test
    public void shouldReadForumNodeFileWithSplits() throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
        conf.set("fs.default.name", "file:///");

        URI uri = getClass().getResource(RESOURCE_DIR_PREFIX + "/forum_node/forum_node.tsv").toURI();
        testFile = new File(uri);
        path = new Path(uri);

        inputFormat = ReflectionUtils.newInstance(CSVFileInputFormat.class, conf);
        context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        long fileSize = 120313812;

        int noSplits = 4;
        List<Long> splitPoints = new ArrayList<Long>();
        for (int i = 0; i < noSplits; i++) {
            splitPoints.add(i * (fileSize / noSplits));
        }
        splitPoints.add(fileSize);

        List<FileSplit> splits = new ArrayList<FileSplit>();
        List<RecordReader> readers = new ArrayList<RecordReader>();
        for (int i = 0; i < splitPoints.size() - 1; i++) {
            long start = splitPoints.get(i);
            if (i != 0) {
                start++;
            }
            long end = splitPoints.get(i + 1);
            FileSplit split = new FileSplit(path, start, end - start, null);
            splits.add(split);
            RecordReader reader = inputFormat.createRecordReader(split, context);
            reader.initialize(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()));
            readers.add(reader);
        }

        org.apache.hadoop.mapreduce.lib.input.LineRecordReader lineReader = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
        lineReader.initialize(new FileSplit(path, 0, testFile.length(), null), context);

        boolean noLineReaderNext = false;

        int i = 0;
        int j = 0;
        while (i < readers.size()) {
            boolean readersHasLine = readers.get(i).nextKeyValue();
            boolean lineReaderHasLine = false;
            if (readersHasLine) {
                lineReaderHasLine = lineReader.nextKeyValue();
            }
            while (readersHasLine && lineReaderHasLine) {
                long aKey = ((LongWritable) readers.get(i).getCurrentKey()).get();
                long bKey = lineReader.getCurrentKey().get();
                Text aValue = (Text) readers.get(i).getCurrentValue();
                Text bValue = lineReader.getCurrentValue();

                if (j == 196987) {
                    System.out.println(j);
                }

                if (bKey == 30082652) {
                    System.out.println(aValue);
                    System.out.println(bValue);
                }

                if (aKey != bKey) {
                    System.out.println(aKey);
                    System.out.println(bKey);
                }
                assertEquals(aKey, bKey);
                assertEquals(aValue, bValue);

                j++;

                if (j == 922569) {
                    System.out.println(j);
                }
                readersHasLine = readers.get(i).nextKeyValue();
                if (readersHasLine) {
                    lineReaderHasLine = lineReader.nextKeyValue();
                }
            }
            i++;
//            if (i < readers.size()-1) {
//                assertEquals(lineReaderHasLine, true);
//            }
        }
        assertEquals(lineReader.nextKeyValue(), false);
//        while (readers.get(i).nextKeyValue()) {
//            //throw
//        }
    }

    @Test
    public void shouldReadFileNoSplits() throws Exception {
        Long firstKey = null;
        Long lastKey = null;

        setUpSplit();
        System.out.println("shouldReadFileNoSplits");
        while (reader.nextKeyValue()) {
            if (firstKey == null) {
                firstKey = ((LongWritable) reader.getCurrentKey()).get();
            }
            System.out.println(reader.getProgress());
            System.out.println(reader.getCurrentKey());
            System.out.println(reader.getCurrentValue());
            lastKey = ((LongWritable) reader.getCurrentKey()).get();
            System.out.println("---");
        }

        assertEquals(firstKey.longValue(), StartOfFirstRecord); //first record
        assertEquals(lastKey.longValue(), EndOfThirdRecord);
    }

    @Test
    public void shouldReadFirstTwoRecords() throws Exception {
        Long firstKey = null;
        Long lastKey = null;

        setUpSplit(0, 72);
        System.out.println("shouldReadFirstRecord");

        while (reader.nextKeyValue()) {
            if (firstKey == null) {
                firstKey = ((LongWritable) reader.getCurrentKey()).get();
            }
            System.out.println(reader.getProgress());
            System.out.println(reader.getCurrentKey());
            System.out.println(reader.getCurrentValue());
            lastKey = ((LongWritable) reader.getCurrentKey()).get();
            System.out.println("---");
        }
        assertEquals(firstKey.longValue(), StartOfFirstRecord);
        assertEquals(lastKey.longValue(), EndOfSecondRecord);
    }

    @Test
    public void shouldReadFileLeftSplitInFirstRecord() throws Exception {
        Long firstKey = null;
        Long lastKey = null;

        setUpSplit(15);
        System.out.println("shouldReadFileLeftSplitInFirstRecord");

        while (reader.nextKeyValue()) {
            if (firstKey == null) {
                firstKey = ((LongWritable) reader.getCurrentKey()).get();
            }
            System.out.println(reader.getProgress());
            System.out.println(reader.getCurrentKey());
            System.out.println(reader.getCurrentValue());
            lastKey = ((LongWritable) reader.getCurrentKey()).get();
            System.out.println("---");
        }
        assertEquals(firstKey.longValue(), StartOfSecondRecord);
        assertEquals(lastKey.longValue(), EndOfThirdRecord);
    }

    @Test
    public void shouldReadFileRightSplitInSecondRecord() throws Exception {
        Long firstKey = null;
        Long lastKey = null;

        setUpSplit(0, 70);
        System.out.println("shouldReadFileRightSplitInSecondRecord");

        while (reader.nextKeyValue()) {
            if (firstKey == null) {
                firstKey = ((LongWritable) reader.getCurrentKey()).get();
            }
            System.out.println(reader.getProgress());
            System.out.println(reader.getCurrentKey());
            System.out.println(reader.getCurrentValue());
            lastKey = ((LongWritable) reader.getCurrentKey()).get();
            System.out.println("---");
        }
        assertEquals(firstKey.longValue(), StartOfFirstRecord);
        assertEquals(lastKey.longValue(), EndOfSecondRecord);
    }

    @Test
    public void shouldReadFileLeftSplitInSecondRecord() throws Exception {
        Long firstKey = null;
        Long lastKey = null;

        setUpSplit(71);
        System.out.println("shouldReadFileLeftSplitInSecondRecord");

        while (reader.nextKeyValue()) {
            if (firstKey == null) {
                firstKey = ((LongWritable) reader.getCurrentKey()).get();
            }
            System.out.println(reader.getProgress());
            System.out.println(reader.getCurrentKey());
            System.out.println(reader.getCurrentValue());
            lastKey = ((LongWritable) reader.getCurrentKey()).get();
            System.out.println("---");
        }
        assertEquals(firstKey.longValue(), StartOfThirdRecord);
        assertEquals(lastKey.longValue(), EndOfThirdRecord);
    }

    @Test
    public void testClose() throws Exception {
        reader.close();
    }

    @Test
    public void testFindNotTokenQuote() throws Exception {
        Assert.assertEquals(CSVRecordReader.findNotTokenQuote(new Text("\"bla"), comma, quote, quote, 0), 0);
    }

    @Test
    public void testFindSynchronizationPoint() throws Exception {
        assertEquals(-1, CSVRecordReader.findSynchronizationPoint(new Text(""), quote, quote, comma));
        assertEquals(-1, CSVRecordReader.findSynchronizationPoint(new Text("bla"), quote, quote, comma));
        assertEquals(-1, CSVRecordReader.findSynchronizationPoint(new Text("bla,bla"), quote, quote, comma));
        assertEquals(-1, CSVRecordReader.findSynchronizationPoint(new Text("bla\n"), quote, quote, comma));
        assertEquals(-1, CSVRecordReader.findSynchronizationPoint(new Text("bla\"\"bla"), quote, quote, comma));
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("\"\"bla"), quote, quote, comma), -1);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("\"bla"), quote, quote, comma), 0);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("b\"la"), quote, quote, comma), -1);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("b\","), quote, quote, comma), 2);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("b,\""), quote, quote, comma), 0);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("b,\"m"), quote, quote, comma), 0);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("Joe Demo,\"2 Demo Street,"), quote, quote, comma), 0);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("Demoville,"), quote, quote, comma), -1);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("Australia. 2615\",joe@someaddress.com"), quote, quote, comma), 16);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("Jim Sample,,jim@sample.com"), quote, quote, comma), -1);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("Jack Example,\"1 Example Street, Exampleville, Australia."), quote, quote, comma), 0);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("2615\",jack@example.com"), quote, quote, comma), 5);
        assertEquals(CSVRecordReader.findSynchronizationPoint(new Text("\",15,\"bla"), quote, quote, comma), 1);
    }

    @Test
    public void shouldPassThroughEmptyLines() throws Exception {
        Queue<Record> queue = new ArrayDeque<Record>();
        queue.add(new Record(0, "\n"));
        queue.add(new Record(1, "\n"));
        queue.add(new Record(2, "\n"));
        LineRecordReader lineRecordReader = new LineRecordReader(queue);
        CSVRecordReader reader = new CSVRecordReader(lineRecordReader, null, null);
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
        reader.initialize(new FileSplit(null, 0, 3, null), new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        int i = 0;
        while (reader.nextKeyValue()) {
            System.out.println(reader.getProgress());
            System.out.println(reader.getCurrentKey());
            System.out.println(reader.getCurrentValue());
//            lastKey = ((LongWritable) reader.getCurrentKey()).get();
            System.out.println("---");
            i++;
        }
        assertEquals(i, 3);

        assertEquals(CSVRecordReader.findNotTokenQuote(new Text("\"bla"), comma, quote, quote, 0), 0);
    }
}
//
//"\"\", \"\"";
//
//"dfasdfd d""
//fasdf  df sdfsd", "sdfgsdf". 564, 546n
//
//class Test {
//    private void test() {
//
//    }
//}
//
//class Uest extends Test {
//    private void uest() {
//        test();
//    }
//}

class LineRecordReader extends org.apache.hadoop.mapreduce.lib.input.LineRecordReader {
    long key;
    String value;
    private Queue<Record> queue;

    public LineRecordReader(Queue<Record> queue) {
        this.queue = queue;
    }

    public LineRecordReader(byte[] recordDelimiter) {
        //pass
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        //pass
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (queue.size() != 0) {
            Record record = queue.remove();
            key = record.getKey();
            value = record.getValue();
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(key);
    }

    @Override
    public Text getCurrentValue() {
        return new Text(value);
    }

    @Override
    public float getProgress() throws IOException {
        return 0.5f;
    }

    @Override
    public synchronized void close() throws IOException {
        //pass
    }
}