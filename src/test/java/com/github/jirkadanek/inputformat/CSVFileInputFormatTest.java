package com.github.jirkadanek.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;

import static org.testng.Assert.assertEquals;

public class CSVFileInputFormatTest {
    static String RESOURCE_DIR_PREFIX = "../../../../";

    private RecordReader reader;
    private File testFile;
    private Path path;
    private Configuration conf;
    private long StartOfFirstRecord = 0;
    private long EndOfFirstRecord = 36;
    private long StartOfSecondRecord = 73;
    private long EndOfSecondRecord = 73;
    private long StartOfThirdRecord = 100;
    private long EndOfThirdRecord = 157;

    @BeforeMethod
    public void setUp() throws Exception {
        conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");

        URI uri = getClass().getResource(RESOURCE_DIR_PREFIX + "teste2.csv").toURI();
        path = new Path(uri);
        testFile = new File(uri);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        reader.close();
    }

    private void setUpSplit() throws Exception {
        long start = 0;
        long end = testFile.length();
        setUpSplit(start, end);
    }

    private void setUpSplit(long start) throws Exception {
        long end = testFile.length() - start;
        setUpSplit(start, end);
    }

    private void setUpSplit(long start, long end) throws Exception {
        long length = end - start;
        FileSplit split = new FileSplit(path, start, length, (String[]) null);
        CSVFileInputFormat inputFormat = ReflectionUtils.newInstance(CSVFileInputFormat.class, conf);
        JobConf job = new JobConf(conf);
        reader = inputFormat.getRecordReader(split, job, null);
    }

    @Test
    public void shouldReadFileNoSplits() throws Exception {
        Long firstKey = null;
        Long lastKey = null;

        setUpSplit();

        LongWritable key = (LongWritable) reader.createKey();
        Text value = (Text) reader.createValue();
        while (reader.next(key, value)) {
            if (firstKey == null) {
                firstKey = key.get();
            }
            System.out.println(reader.getProgress());
            System.out.println(key);
            System.out.println(value);
            lastKey = key.get();
            System.out.println("---");
        }

        assertEquals(firstKey.longValue(), StartOfFirstRecord); //first record
        assertEquals(lastKey.longValue(), EndOfThirdRecord);
    }

    @Test
    public void testGetSplits() throws Exception {

    }

    @Test
    public void testGetRecordReader() throws Exception {

    }
}