package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Mapper that fails
public class FailMapper extends MapReduceBase implements
    Mapper<WritableComparable, Writable, WritableComparable, Writable> {

  public void map(WritableComparable key, Writable value,
      OutputCollector<WritableComparable, Writable> out, Reporter reporter)
      throws IOException {
    // NOTE- the next line is required for the TestDebugScript test to succeed
    System.err.println("failing map");
    throw new RuntimeException("failing map");
  }
}
