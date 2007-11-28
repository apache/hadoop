/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.shell;

import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.hbase.shell.algebra.OperationEvaluator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

/**
 * Save results to specified table.
 */
public class SaveCommand extends BasicCommand {
  private String chainKey;
  private String output;

  public SaveCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(HBaseConfiguration conf) {
    try {
      HConnection conn = HConnectionManager.getConnection(conf);

      if (!conn.tableExists(new Text(output))) {
        OperationEvaluator eval = new OperationEvaluator(conf, chainKey, output);
        JobConf jobConf = eval.getJobConf();
        if (submitJob(jobConf)) {
          return new ReturnMsg(0, "Successfully complete.");
        } else {
          HBaseAdmin admin = new HBaseAdmin(conf);
          admin.deleteTable(new Text(output));

          return new ReturnMsg(0, "Job failed.");
        }
      } else {
        return new ReturnMsg(0, "'" + output + "' table already exist.");
      }

    } catch (IOException e) {
      return new ReturnMsg(0, e.toString());
    }
  }

  /**
   * Submit a job to job tracker.
   * 
   * @param job
   * @return result
   * @throws IOException
   */
  public boolean submitJob(JobConf job) throws IOException {
    JobClient jc = new JobClient(job);
    boolean success = true;
    RunningJob running = null;
    try {
      running = jc.submitJob(job);
      String jobId = running.getJobID();

      while (!running.isComplete()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
        }
        running = jc.getJob(jobId);
      }
      success = running.isSuccessful();
    } finally {
      if (!success && (running != null)) {
        running.killJob();
      }
      jc.close();
    }
    return success;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public void setStatement(String chainKey) {
    this.chainKey = chainKey;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SHELL;
  }
}
