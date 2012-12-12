/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;

/**
 * This is a helper class that is used by test cases to run a high ram job
 * the intention behind creatint this class is reuse code. 
 *
 */
public class HighRamJobHelper {

  public  HighRamJobHelper () {
    
  }
  
  /**
   * The method runs the high ram job
   * @param conf configuration for unning the job
   * @param jobClient instance
   * @param remoteJTClient instance
   * @return the job id of the high ram job
   * @throws Exception is thrown when the method fails to run the high ram job
   */
  public JobID runHighRamJob (Configuration conf, JobClient jobClient, 
      JTProtocol remoteJTClient,String assertMessage) throws Exception {
    SleepJob job = new SleepJob();
    String jobArgs []= {"-D","mapred.cluster.max.map.memory.mb=2048", 
                        "-D","mapred.cluster.max.reduce.memory.mb=2048", 
                        "-D","mapred.cluster.map.memory.mb=1024", 
                        "-D","mapreduce.job.complete.cancel.delegation.tokens=false",
                        "-D","mapred.cluster.reduce.memory.mb=1024",
                        "-m", "6", 
                        "-r", "2", 
                        "-mt", "2000", 
                        "-rt", "2000",
                        "-recordt","100"};
    JobConf jobConf = new JobConf(conf);
    jobConf.setMemoryForMapTask(2048);
    jobConf.setMemoryForReduceTask(2048);
    int exitCode = ToolRunner.run(jobConf, job, jobArgs);
    Assert.assertEquals("Exit Code:", 0, exitCode);
    UtilsForTests.waitFor(1000); 
    JobID jobId = jobClient.getAllJobs()[0].getJobID();
    JobInfo jInfo = remoteJTClient.getJobInfo(jobId);
    Assert.assertEquals(assertMessage, 
        jInfo.getStatus().getRunState(), JobStatus.SUCCEEDED);
    return jobId;
  }
  
}
