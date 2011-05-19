package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Write table content out to files in hdfs.
 */
public class TsvImporterMapper
extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
{

  /** Timestamp for all inserted rows */
  private long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;

  private ImportTsv.TsvParser parser;

  public long getTs() {
    return ts;
  }

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   * Common initialization that might be leveraged by a subsclass is done in
   * <code>doSetup</code>. Hence a subclass may choose to override this method
   * and call <code>doSetup</code> as well before handling it's own custom params.
   *
   * @param context
   */
  @Override
  protected void setup(Context context) {
    doSetup(context);

    Configuration conf = context.getConfiguration();

    parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY),
                           separator);
    if (parser.getRowKeyColumnIndex() == -1) {
      throw new RuntimeException("No row key column specified");
    }
  }

  /**
   * Handles common parameter initialization that a subclass might want to leverage.
   * @param context
   */
  protected void doSetup(Context context) {
    Configuration conf = context.getConfiguration();

    // If a custom separator has been used,
    // decode it back from Base64 encoding.
    separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
    if (separator == null) {
      separator = ImportTsv.DEFAULT_SEPARATOR;
    } else {
      separator = new String(Base64.decode(separator));
    }

    ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, System.currentTimeMillis());

    skipBadLines = context.getConfiguration().getBoolean(
        ImportTsv.SKIP_LINES_CONF_KEY, true);
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");
  }

  /**
   * Convert a line of TSV text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value,
    Context context)
  throws IOException {
    byte[] lineBytes = value.getBytes();

    try {
      ImportTsv.TsvParser.ParsedLine parsed = parser.parse(
          lineBytes, value.getLength());
      ImmutableBytesWritable rowKey =
        new ImmutableBytesWritable(lineBytes,
            parsed.getRowKeyOffset(),
            parsed.getRowKeyLength());

      Put put = new Put(rowKey.copyBytes());
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        if (i == parser.getRowKeyColumnIndex()) continue;
        KeyValue kv = new KeyValue(
            lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
            parser.getFamily(i), 0, parser.getFamily(i).length,
            parser.getQualifier(i), 0, parser.getQualifier(i).length,
            ts,
            KeyValue.Type.Put,
            lineBytes, parsed.getColumnOffset(i), parsed.getColumnLength(i));
        put.add(kv);
      }
      context.write(rowKey, put);
    } catch (ImportTsv.TsvParser.BadTsvLineException badLine) {
      if (skipBadLines) {
        System.err.println(
            "Bad line at offset: " + offset.get() + ":\n" +
            badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (IllegalArgumentException e) {
      if (skipBadLines) {
        System.err.println(
            "Bad line at offset: " + offset.get() + ":\n" +
            e.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(e);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
