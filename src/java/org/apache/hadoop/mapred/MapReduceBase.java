package org.apache.hadoop.mapred;

import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.mapred.JobConfigurable;

/**
 * A class to implement the trivial close and configure methods.
 * @author Owen O'Malley
 */
public class MapReduceBase implements Closeable, JobConfigurable {

  public void close() {
  }

  public void configure(JobConf job) {
  }

}
