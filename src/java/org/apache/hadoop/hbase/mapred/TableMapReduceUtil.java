package org.apache.hadoop.hbase.mapred;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("unchecked")
public class TableMapReduceUtil {
  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   * 
   * @param table table name
   * @param columns columns to scan
   * @param mapper mapper class
   * @param outputKeyClass
   * @param outputValueClass
   * @param job job configuration
   */
  public static void initTableMapJob(String table, String columns,
    Class<? extends TableMap> mapper, 
    Class<? extends WritableComparable> outputKeyClass, 
    Class<? extends Writable> outputValueClass, JobConf job) {
      
    job.setInputFormat(TableInputFormat.class);
    job.setMapOutputValueClass(outputValueClass);
    job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    FileInputFormat.addInputPaths(job, table);
    job.set(TableInputFormat.COLUMN_LIST, columns);
  }
  
  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initTableReduceJob(String table,
      Class<? extends TableReduce> reducer, JobConf job) {
    job.setOutputFormat(TableOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(BatchUpdate.class);
  }
}
