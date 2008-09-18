/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * <code>JobQueueClient</code> is interface provided to the user in order
 * to get JobQueue related information from the {@link JobTracker}
 * 
 * It provides the facility to list the JobQueues present and ability to 
 * view the list of jobs within a specific JobQueue 
 * 
**/

class JobQueueClient extends Configured implements  Tool {
  
  JobClient jc;

  
  public JobQueueClient() {
  }
  
  public JobQueueClient(JobConf conf) throws IOException {
    setConf(conf);
  }
  
  private void init(JobConf conf) throws IOException {
    setConf(conf);
    jc = new JobClient(conf);
  }
  
  @Override
  public int run(String[] argv) throws Exception {
    int exitcode = -1;
    
    if(argv.length < 1){
      displayUsage("");
      return exitcode;
    }
    String cmd = argv[0];
    boolean displayQueueList = false;
    boolean displayQueueInfoWithJobs = false;
    boolean displayQueueInfoWithoutJobs = false;
    
    if("-list".equals(cmd)){
      displayQueueList = true;
    }else if("-info".equals(cmd)){
      if(argv.length == 2 && !(argv[1].equals("-showJobs"))) {
        displayQueueInfoWithoutJobs = true;
      } else if(argv.length == 3){
        if(argv[2].equals("-showJobs")){
          displayQueueInfoWithJobs = true;
        }else {
          displayUsage(cmd);
          return exitcode;
        }
      }else {
        displayUsage(cmd);
        return exitcode;
      }      
    } else {
      displayUsage(cmd);
      return exitcode;
    }
    JobConf conf = new JobConf(getConf());
    init(conf);
    if (displayQueueList) {
      displayQueueList();
      exitcode = 0;
    } else if (displayQueueInfoWithoutJobs){
      displayQueueInfo(argv[1],false);
      exitcode = 0;
    } else if (displayQueueInfoWithJobs) {
      displayQueueInfo(argv[1],true);
      exitcode = 0;
    }
    
    return exitcode;
  }
  
  /**
   * Method used to display information pertaining to a Single JobQueue 
   * registered with the {@link QueueManager}. Display of the Jobs is 
   * determine by the boolean 
   * 
   * @throws IOException
   */

  private void displayQueueInfo(String queue, boolean showJobs) throws IOException {
    JobQueueInfo schedInfo = jc.getQueueInfo(queue);
    if (schedInfo == null) {
      System.out.printf("Queue Name : %s has no scheduling information \n", queue);
    } else {
      System.out.printf("Queue Name : %s \n", schedInfo.getQueueName());
      System.out.printf("Scheduling Info : %s \n",schedInfo.getSchedulingInfo());
    }
    if (showJobs) {
      System.out.printf("Job List\n");
      JobStatus[] jobs = jc.getJobsFromQueue(queue);
      if (jobs == null)
        jobs = new JobStatus[0];
      jc.displayJobList(jobs);
    }
  }

  /**
   * Method used to display the list of the JobQueues registered
   * with the {@link QueueManager}
   * 
   * @throws IOException
   */
  private void displayQueueList() throws IOException {
    JobQueueInfo[] queues = jc.getQueues();
    for (JobQueueInfo queue : queues) {
      String schedInfo = queue.getSchedulingInfo();
      if(schedInfo.trim().equals("")){
        schedInfo = "N/A";
      }
      System.out.printf("Queue Name : %s \n", queue.getQueueName());
      System.out.printf("Scheduling Info : %s \n",queue.getSchedulingInfo());
    }
  }

  private void displayUsage(String cmd) {
    String prefix = "Usage: JobQueueClient ";
    if ("-queueinfo".equals(cmd)){
      System.err.println(prefix + "[" + cmd + "<job-queue-name> [-showJobs]]");
    }else {
      System.err.printf(prefix + "<command> <args>\n");
      System.err.printf("\t[-list]\n");
      System.err.printf("\t[-info <job-queue-name> [-showJobs]]\n\n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new JobQueueClient(), argv);
    System.exit(res);
  }
  
}
