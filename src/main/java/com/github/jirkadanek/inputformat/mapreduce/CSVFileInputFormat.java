package com.github.jirkadanek.inputformat.mapreduce;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * # Usage
 * <p/>
 * # Configuration options
 * csvinputformat.record.delimiter // record delimiter (not _field_ delimiter), probably '\n'
 * csvinputformat.record.quote // quote character, probably '"'
 * for CSVRecordReader:
 * csvrecordreader.record.maxlength
 * for LineRecordReader:
 * mapreduce.input.linerecordreader.line.maxlength
 * <p/>
 * <p/>
 * getKey and getValue() may return null when nextKeyValue == false
 * value returned from getKey is mutable and valid only until the next call to nextKeyValue is made
 * <p/>
 * <p/>
 * Based on information from
 * https://stackoverflow.com/questions/16269922/hadoop-mapred-vs-hadoop-mapreduce
 */
public class CSVFileInputFormat extends TextInputFormat {
    static String CONFIGURATION_DELIMITER = "csvinputformat.record.delimiter";
    static String CONFIGURATION_QUOTE = "csvinputformat.record.quote";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get(CONFIGURATION_DELIMITER);
        String quote = context.getConfiguration().get(CONFIGURATION_QUOTE);
        byte[] recordDelimiterBytes = null;
        byte[] recordQuoteBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        if (null != quote) {
            recordQuoteBytes = quote.getBytes(Charsets.UTF_8);
        }
        return new CSVRecordReader(recordDelimiterBytes, recordQuoteBytes);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return super.isSplitable(context, file);
    }
}

/**
 * Record reader that reads CSV records. It is implemented as an adapter around LineRecordReader that is aware of CSV record boundaries
 * and can handle multiline records.
 * <p/>
 * # Idea
 * <p/>
 * Do for records what LineRecordReader does for lines
 * https://stackoverflow.com/questions/14291170/how-does-hadoop-process-records-records-split-across-block-boundaries
 * <p/>
 * 1. Get LineRecordReader, use it to read the input, drop everything until the start of a first record
 * 2. Read subsequent records
 * 3. Finish after reading one record past the end of the split
 */
class CSVRecordReader extends RecordReader<LongWritable, Text> {
    public static final String MAX_RECORD_LENGTH = "csvrecordreader.record.maxlength";
    public static final String MAX_RECORD_SYNCHRONIZE_LENGTH = "csvrecordreader.record.maxsynchronizelength";
    int escape = "\"".codePointAt(0);
    int quote = "\"".codePointAt(0);
    int separator = ",".codePointAt(0);
    long start;
    long end;
    long pos;
    LongWritable key;
    Text value;
    Queue<Record> queue;
    long enqueuedSize = 0;
    boolean noNextKeyValue = false;
    private LineRecordReader lineRecordReader;
    private byte[] recordDelimiter;
    private int maxRecordLength;
    private int maxRecordSynchronizeLength;
    private FileSplit fileSplit;
    private FileSplit fakeSplit;

    CSVRecordReader(byte[] recordDelimiterBytes, byte[] quoteSequenceBytes) {
        this.recordDelimiter = recordDelimiterBytes;
        //this.recordQuote = quoteSequenceBytes;
    }

    /// Inject a LineRecordReader, for unit testing
    CSVRecordReader(LineRecordReader fakeRecordReader, byte[] recordDelimiterBytes, byte[] quoteSequenceBytes) {
        this(recordDelimiterBytes, quoteSequenceBytes);
        this.lineRecordReader = fakeRecordReader;
    }

    /**
     * @param text
     * @param separator
     * @param escape
     * @param quote
     * @param i         @return position of the quote in the text
     */
    static int findNotTokenQuote(Text text, int separator, int escape, int quote, int i) {
        String string = new String(Character.toChars(quote));
        while ((i = text.find(string, i)) != -1) {
            if (quote != escape) {
                if (i == 0) {
                    return i;
                }
                if (text.charAt(i - 1) != escape /*&& text.charAt(i - 1) != separator*/) {
                    return i;
                }
                i++;
            } else {
                int k = 1;
                int length = text.getLength();
                while (i + k < length && text.charAt(i + k) == quote) {
                    k++;
                }
                //if(i>0 && text.charAt(i-1) != separator) {
                i = i + k - 1;
                if (k % 2 != 0) {
                    // odd number of quotes not preceded by separator
                    return i;
                }
                //}
                i = i + k - 1;
            }
        }
        return -1;
    }

    static int findEndOfQuotedField(Text text, int escape, int quote, int separator, int i) {
        int length = text.getLength();
        while ((i = findNotTokenQuote(text, separator, escape, quote, i)) != -1) {
            if (i == length - 1) {
                return i;
            }
            if (text.charAt(i + 1) == separator) {
                return i + 1;
            }
            i++;
        }
        return -1;
    }

    static int findSynchronizationPoint(Text text, int escape, int quote, int separator) {
        int i = 0;
        i = findNotTokenQuote(text, separator, escape, quote, i);
        if (i != -1) {
            int k = 0;
            while (k <= i && (text.charAt(i - k) == quote || text.charAt(i - k) == escape)) {
                k++;
            }
            if (i + 1 == text.getLength() || text.charAt(i + 1) != separator) {
                // line begins with a start of quoted field
                if (i == k - 1) {

                    return 0;
                }
                // this is the end of a quoted field, a record starts on this line
                // neither the first, otherwise it would never get to this point
                // bla," OK
                // bla", NOK
                if (text.charAt(i - 1) == separator) {
                    return 0;
                }
            }
            //FIXME: ,",\n
            // "la
            // bla,"
            // ,",fdsss
            // quoted field starts or ends here, may end somewhere in this line
            if ((i = findEndOfQuotedField(text, escape, quote, separator, i)) != -1) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration job = context.getConfiguration();
        this.maxRecordLength = job.getInt(MAX_RECORD_LENGTH, Integer.MAX_VALUE);
        this.maxRecordSynchronizeLength = job.getInt(MAX_RECORD_SYNCHRONIZE_LENGTH, 1024 * 1024);

        this.fileSplit = (FileSplit) genericSplit;

        if (this.lineRecordReader == null) {
            this.fakeSplit = new FileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength() + maxRecordLength, fileSplit.getLocations());

            // from TextInputFormat
            String delimiter = context.getConfiguration().get(
                    "textinputformat.record.delimiter");
            byte[] recordDelimiterBytes = null;
            if (null != delimiter)
                recordDelimiterBytes = delimiter.getBytes(com.google.common.base.Charsets.UTF_8);
            this.lineRecordReader = new LineRecordReader(recordDelimiterBytes);
        }
        lineRecordReader.initialize(fakeSplit, context);

        this.start = fileSplit.getStart();
        this.end = start + fileSplit.getLength();
        this.pos = getFilePosition();

        queue = new ArrayDeque<Record>();
        // should I perform such a long operation inside initialize() method?
        if (start != 0) {
            //it is possible the split is inside a quoted field
            if (synchronizeParser()) {
                //synchronization point was found
                queue.clear();
                this.pos = getFilePosition();
            }
        } else {
            lineRecordReader.nextKeyValue();
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new Text();
        }

        if (queue.size() != 0) {
            Record record = queue.remove();
            key.set(record.getKey());
            value.set(record.getValue());
            pos = key.get();

            return true;
        }

        if (noNextKeyValue) {
            return false;
        }

//        if (lineRecordReader.nextKeyValue()) { //jo, dobre
        key.set(lineRecordReader.getCurrentKey().get());
        value.set(lineRecordReader.getCurrentValue());
        pos = key.get();

        //if (lineRecordReader.getProgress() >= 1.0f) { // look at the float value from linereader, does not work
        if (pos >= end) { // breaks for compressed files
            // see where the next mapper will start reading the file
            // in order to process the whole file without gaps, everything up to that point must be processed by this mapper

            enqueuedSize = 0;
            enqueueLine(key, value);
            boolean dropQueue = !synchronizeParser(); // also enqueue the lines
            Record record = queue.remove();
            key.set(record.getKey());
            value.set(record.getValue());
            if (dropQueue) {
                queue.clear();
            }
            noNextKeyValue = true;
            return true;
        }

        noNextKeyValue = !lineRecordReader.nextKeyValue();
        return true;
//            return true;
//        }

//        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (lineRecordReader != null) {
            lineRecordReader.close();
        }
    }

    /**
     * Hack, suffers from floating point arithmetic, but seems to work in practice
     */
    private long getFilePosition() throws IOException, InterruptedException {
        if (fakeSplit != null) {
            long start = fakeSplit.getStart();
            long end = start + fakeSplit.getLength();
            long filePosition = (long) (lineRecordReader.getProgress() * (float) (end - start)) + start;
            return filePosition;
        }
        return pos;
    }

    private boolean enqueueLine(LongWritable key, Text line) {
        queue.add(new Record(key.get(), line.toString()));
        enqueuedSize += line.getLength();
        return enqueuedSize <= maxRecordSynchronizeLength;
    }

    /*
    lineRecordReader is left pointing at the next line that should be processed
    if there is no such record, noNextKeyValue is set to true
    queue contains all lines from the start of reading up to the synchronization point
     */
    private boolean synchronizeParser() throws IOException, InterruptedException {
        while (lineRecordReader.nextKeyValue()) {
            Text line = lineRecordReader.getCurrentValue();
            int i = findSynchronizationPoint(line, escape, quote, separator);
            if (i == -1) {
                //not found, enqueue current line and try next line
                if (!enqueueLine(lineRecordReader.getCurrentKey(), line)) {
                    //queue is over capacity
                    return false;
                }
            } else {
                if (i == 0) {
                    //synchronized at the beginning of a new record
                    return true;
                }
                if (i == line.getLength() - 1) {
                    //synchronized at the end of a record
                    // this line belongs to the previous part, next line belongs to this part
                    enqueueLine(lineRecordReader.getCurrentKey(), line);
                    noNextKeyValue = !lineRecordReader.nextKeyValue();
                    return true;
                }
                // not covered by tests?
                // synchronized at the end of a quoted field, but inside of a record
                return consumeUntilStartOfRecord(line, i);
            }
        }
        noNextKeyValue = true;
        return true;

//        <!escape><quote><separator> end of field
//        <!escape><quote><newline> end of record
//        if I know I am out of quoted field, <newline> is enough
    }

    /**
     * unquoted newline
     *
     * @param line
     * @param i
     * @return
     * @throws IOException
     */
    private boolean consumeUntilStartOfRecord(Text line, int i) throws IOException {
        boolean insideQuotedField = false;
        do {
            while ((i = findNotTokenQuote(line, separator, escape, quote, i)) != -1) {
                insideQuotedField = !insideQuotedField;
                i++;
            }
            if (insideQuotedField == false) {
                //start of a new record on the next line
                // this line belongs to the previous record
                enqueueLine(lineRecordReader.getCurrentKey(), line);
                noNextKeyValue = !lineRecordReader.nextKeyValue();
                return true;
            }
            if (!enqueueLine(lineRecordReader.getCurrentKey(), line)) {
                //if the file is properly formatted, the record must end eventually
                //it might be smart to ignore the maxRecordSynchronizeLength here to prevent data loss
                //or have a larger limit for this case
                //FIXME: noNextKeyValue = !lineRecordReader.nextKeyValue();
                return false;
            }
            i = 0;
        } while (lineRecordReader.nextKeyValue());
        noNextKeyValue = true;
        return false;
    }


}

class Record {
    long key;
    String value;

    Record(long key, String value) {
        this.key = key;
        this.value = value;
    }

    public long getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record record = (Record) o;

        if (key != record.key) return false;
        return value.equals(record.value);

    }

    @Override
    public int hashCode() {
        int result = (int) (key ^ (key >>> 32));
        result = 31 * result + value.hashCode();
        return result;
    }
}