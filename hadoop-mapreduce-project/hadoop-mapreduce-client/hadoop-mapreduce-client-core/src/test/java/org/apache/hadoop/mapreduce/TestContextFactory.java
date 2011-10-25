package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.junit.Before;
import org.junit.Test;

public class TestContextFactory {

  JobID jobId;
  Configuration conf;
  JobContext jobContext;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    jobId = new JobID("test", 1);
    jobContext = new JobContextImpl(conf, jobId);
  }
  
  @Test
  public void testCloneContext() throws Exception {
    ContextFactory.cloneContext(jobContext, conf);
  }

  @Test
  public void testCloneMapContext() throws Exception {
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
    TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);
    MapContext<IntWritable, IntWritable, IntWritable, IntWritable> mapContext =
    new MapContextImpl<IntWritable, IntWritable, IntWritable, IntWritable>(
        conf, taskAttemptid, null, null, null, null, null);
    Mapper<IntWritable, IntWritable, IntWritable, IntWritable>.Context mapperContext = 
      new WrappedMapper<IntWritable, IntWritable, IntWritable, IntWritable>().getMapContext(
          mapContext);
    ContextFactory.cloneMapContext(mapperContext, conf, null, null);
  }

  @Before
  public void tearDown() throws Exception {
    
  }
}
