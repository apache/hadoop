package org.apache.hadoop.mapred.gridmix.test.system;

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.junit.Assert;

/**
 * Submit the gridmix jobs. 
 */
public class GridmixJobSubmission {
  private static final Log LOG = LogFactory.
     getLog(GridmixJobSubmission.class);
  private int gridmixJobCount;
  private Configuration conf;
  private Path gridmixDir;
  private JTClient jtClient;

  public GridmixJobSubmission(Configuration conf,JTClient jtClient , 
    Path gridmixDir) {
    this.conf = conf;
    this.jtClient = jtClient;
    this.gridmixDir = gridmixDir;
  }
  
  /**
   * Submit the gridmix jobs.
   * @param runtimeArgs - gridmix common runtime arguments.
   * @param otherArgs - gridmix other runtime arguments.
   * @param traceInterval - trace time interval.
   * @throws Exception
   */
  public void submitJobs(String [] runtimeArgs,
     String [] otherArgs, int mode) throws Exception {
    int prvJobCount = jtClient.getClient().getAllJobs().length;
    int exitCode = -1;
    if (otherArgs == null) {
      exitCode = UtilsForGridmix.runGridmixJob(gridmixDir,
          conf, mode,
          runtimeArgs);
    } else {
      exitCode = UtilsForGridmix.runGridmixJob(gridmixDir,
          conf, mode,
          runtimeArgs, otherArgs);
    }
    Assert.assertEquals("Gridmix jobs have failed.", 0 , exitCode);
    gridmixJobCount = jtClient.getClient().getAllJobs().length;
    gridmixJobCount -= prvJobCount;
  }
  /**
   * Get the submitted jobs count.
   * @return count of no. of jobs submitted for a trace.
   */
  public int getGridmixJobCount() {
     return gridmixJobCount;
  }
  /**
   * Get the job configuration.
   * @return Configuration of a submitted job.
   */
  public Configuration getJobConf() {
    return conf;
  }
}
