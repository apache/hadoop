package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import junit.framework.TestCase;

public class TestJobInProgress extends TestCase {

  private MiniMRCluster mrCluster;

  private MiniDFSCluster dfsCluster;
  JobTracker jt;

  public static class FailMapTaskJob extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      // reporter.incrCounter(TaskCounts.LaunchedTask, 1);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalArgumentException("Interrupted MAP task");
      }
      throw new IllegalArgumentException("Failing MAP task");
    }
  }

  // Suppressing waring as we just need to write a failing reduce task job
  // We don't need to bother about the actual key value pairs which are passed.
  @SuppressWarnings("unchecked")
  public static class FailReduceTaskJob extends MapReduceBase implements
      Reducer {

    @Override
    public void reduce(Object key, Iterator values, OutputCollector output,
        Reporter reporter) throws IOException {
      // reporter.incrCounter(TaskCounts.LaunchedTask, 1);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalArgumentException("Failing Reduce task");
      }
      throw new IllegalArgumentException("Failing Reduce task");
    }

  }

  @Override
  protected void setUp() throws Exception {
    // TODO Auto-generated method stub
    super.setUp();
    final int taskTrackers = 4;
    Configuration conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, 4, true, null);
    mrCluster = new MiniMRCluster(taskTrackers, dfsCluster.getFileSystem()
        .getUri().toString(), 1);
    jt = mrCluster.getJobTrackerRunner().getJobTracker();
  }

  public void testPendingMapTaskCount() throws Exception {
    launchTask(FailMapTaskJob.class, IdentityReducer.class);
    checkTaskCounts();
  }
  
  public void testPendingReduceTaskCount() throws Exception {
    launchTask(IdentityMapper.class, FailReduceTaskJob.class);
    checkTaskCounts();
  }

  @Override
  protected void tearDown() throws Exception {
    mrCluster.shutdown();
    dfsCluster.shutdown();
    super.tearDown();
  }
  

  @SuppressWarnings("unchecked")
  void launchTask(Class MapClass,Class ReduceClass) throws Exception{
    JobConf jobConf = mrCluster.createJobConf();

    JobClient jc = new JobClient(jobConf);
    final Path inDir = new Path("./failjob/input");
    final Path outDir = new Path("./failjob/output");
    String input = "Test failing job.\n One more line";
    FileSystem inFs = inDir.getFileSystem(jobConf);
    FileSystem outFs = outDir.getFileSystem(jobConf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("create directory failed" + inDir.toString());
    }

    DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
    file.writeBytes(input);
    file.close();
    jobConf.setJobName("failmaptask");
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMapperClass(MapClass);
    jobConf.setCombinerClass(ReduceClass);
    jobConf.setReducerClass(ReduceClass);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    jobConf.setNumMapTasks(10);
    jobConf.setNumReduceTasks(5);
    RunningJob job = null;
    try {
      job = JobClient.runJob(jobConf);
    } catch (IOException e) {
    }

  }

  void checkTaskCounts() {
    JobStatus[] status = jt.getAllJobs();
    for (JobStatus js : status) {
      JobInProgress jip = jt.getJob(js.getJobID());
      Counters counter = jip.getJobCounters();
      long totalTaskCount = counter
          .getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS)
          + counter.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES);
      while (jip.getNumTaskCompletionEvents() < totalTaskCount) {
        assertEquals(true, (jip.runningMaps() >= 0));
        assertEquals(true, (jip.pendingMaps() >= 0));
        assertEquals(true, (jip.runningReduces() >= 0));
        assertEquals(true, (jip.pendingReduces() >= 0));
      }
    }
  }
  
}
