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
import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * <code>JobQueueClient</code> is interface provided to the user in order to get
 * JobQueue related information from the {@link JobTracker}
 * 
 * It provides the facility to list the JobQueues present and ability to view
 * the list of jobs within a specific JobQueue
 * 
 **/

class JobQueueClient extends Configured implements Tool {

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

    if (argv.length < 1) {
      displayUsage("");
      return exitcode;
    }
    String cmd = argv[0];
    boolean displayQueueList = false;
    boolean displayQueueInfoWithJobs = false;
    boolean displayQueueInfoWithoutJobs = false;
    boolean displayQueueAclsInfoForCurrentUser = false;

    if ("-list".equals(cmd)) {
      displayQueueList = true;
    } else if ("-showacls".equals(cmd)) {
      displayQueueAclsInfoForCurrentUser = true;
    } else if ("-info".equals(cmd)) {
      if (argv.length == 2 && !(argv[1].equals("-showJobs"))) {
        displayQueueInfoWithoutJobs = true;
      } else if (argv.length == 3) {
        if (argv[2].equals("-showJobs")) {
          displayQueueInfoWithJobs = true;
        } else {
          displayUsage(cmd);
          return exitcode;
        }
      } else {
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
    } else if (displayQueueInfoWithoutJobs) {
      displayQueueInfo(argv[1], false);
      exitcode = 0;
    } else if (displayQueueInfoWithJobs) {
      displayQueueInfo(argv[1], true);
      exitcode = 0;
    } else if (displayQueueAclsInfoForCurrentUser) {
      this.displayQueueAclsInfoForCurrentUser();
      exitcode = 0;
    }
    return exitcode;
  }

  // format and print information about the passed in job queue.
  void printJobQueueInfo(JobQueueInfo jobQueueInfo, Writer writer)
      throws IOException {
    if (jobQueueInfo == null) {
      writer.write("No queue found.\n");
      writer.flush();
      return;
    }
    writer.write(String.format("Queue Name : %s \n",
        jobQueueInfo.getQueueName()));
    writer.write(String.format("Queue State : %s \n",
        jobQueueInfo.getQueueState()));
    writer.write(String.format("Scheduling Info : %s \n",
        jobQueueInfo.getSchedulingInfo()));
    List<JobQueueInfo> childQueues = jobQueueInfo.getChildren();
    if (childQueues != null && childQueues.size() > 0) {
      writer.write(String.format("Child Queues : "));
      for (int i = 0; i < childQueues.size(); i++) {
        JobQueueInfo childQueue = childQueues.get(i);
        writer.write(String.format("%s", childQueue.getQueueName()));
        if (i != childQueues.size() - 1) {
          writer.write(String.format(", "));
        }
      }
      writer.write("\n");
    }
    writer.write(String.format("======================\n"));
    writer.flush();
  }
  
  private void displayQueueList() throws IOException {
    JobQueueInfo[] rootQueues = jc.getRootQueues();
    List<JobQueueInfo> allQueues = expandQueueList(rootQueues);
    for (JobQueueInfo queue : allQueues) {
      printJobQueueInfo(queue, new PrintWriter(System.out));
    }
  }
  
  /**
   * Expands the hierarchy of queues and gives the list of all queues in 
   * depth-first order
   * @param rootQueues the top-level queues
   * @return the list of all the queues in depth-first order.
   */
  List<JobQueueInfo> expandQueueList(JobQueueInfo[] rootQueues) {
    List<JobQueueInfo> allQueues = new ArrayList<JobQueueInfo>();
    for (JobQueueInfo queue : rootQueues) {
      allQueues.add(queue);
      if (queue.getChildren() != null) {
        JobQueueInfo[] childQueues 
          = queue.getChildren().toArray(new JobQueueInfo[0]);
        allQueues.addAll(expandQueueList(childQueues));
      }
    }
    return allQueues;
  }
 
  /**
   * Method used to display information pertaining to a Single JobQueue
   * registered with the {@link QueueManager}. Display of the Jobs is determine
   * by the boolean
   * 
   * @throws IOException
   */
  private void displayQueueInfo(String queue, boolean showJobs)
      throws IOException {
    JobQueueInfo jobQueueInfo = jc.getQueueInfo(queue);
    
    if (jobQueueInfo == null) {
      System.out.println("Queue \"" + queue + "\" does not exist.");
      return;
    }
    printJobQueueInfo(jobQueueInfo, new PrintWriter(System.out));
    if (showJobs && (jobQueueInfo.getChildren() == null ||
        jobQueueInfo.getChildren().size() == 0)) {
      JobStatus[] jobs = jc.getJobsFromQueue(queue);
      if (jobs == null)
        jobs = new JobStatus[0];
      jc.displayJobList(jobs);
    }
  }
   
  private void displayQueueAclsInfoForCurrentUser() throws IOException {
    QueueAclsInfo[] queueAclsInfoList = jc.getQueueAclsForCurrentUser();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    if (queueAclsInfoList.length > 0) {
      System.out.println("Queue acls for user :  " + ugi.getShortUserName());
      System.out.println("\nQueue  Operations");
      System.out.println("=====================");
      for (QueueAclsInfo queueInfo : queueAclsInfoList) {
        System.out.print(queueInfo.getQueueName() + "  ");
        String[] ops = queueInfo.getOperations();
        int max = ops.length - 1;
        for (int j = 0; j < ops.length; j++) {
          System.out.print(ops[j].replaceFirst("acl-", ""));
          if (j < max) {
            System.out.print(",");
          }
        }
        System.out.println();
      }
    } else {
      System.out.println("User " + ugi.getShortUserName()
          + " does not have access to any queue. \n");
    }
  }

  private void displayUsage(String cmd) {
    String prefix = "Usage: JobQueueClient ";
    if ("-queueinfo".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "<job-queue-name> [-showJobs]]");
    } else {
      System.err.printf(prefix + "<command> <args>\n");
      System.err.printf("\t[-list]\n");
      System.err.printf("\t[-info <job-queue-name> [-showJobs]]\n");
      System.err.printf("\t[-showacls] \n\n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new JobQueueClient(), argv);
    System.exit(res);
  }

}
