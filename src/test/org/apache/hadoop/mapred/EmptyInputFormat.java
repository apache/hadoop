package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

/**
  * InputFormat which simulates the absence of input data
  * by returning zero split.
  */
public class EmptyInputFormat extends InputFormatBase {

  public FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits) throws IOException {
    return new FileSplit[0];
  }

  public RecordReader getRecordReader(FileSystem fs, FileSplit split, JobConf job, Reporter reporter) throws IOException {
    return new SequenceFileRecordReader(job, split);
  }
}