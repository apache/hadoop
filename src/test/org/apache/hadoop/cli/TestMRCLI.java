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

package org.apache.hadoop.cli;

import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.cli.util.CLITestData.TestCmd;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.util.ToolRunner;

public class TestMRCLI extends TestHDFSCLI{

  protected MiniMRCluster mrCluster = null;
  protected String jobtracker = null;
  protected MRCmdExecutor cmdExecutor = null;
  
  public void setUp() throws Exception {
    super.setUp();
    JobConf mrConf = new JobConf(conf);
    mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri().toString(), 1, 
                           null, null, mrConf);
    jobtracker = mrCluster.createJobConf().get("mapred.job.tracker", "local");
    cmdExecutor = new MRCmdExecutor(jobtracker);
  }

  
  public void tearDown() throws Exception {
    mrCluster.shutdown();
    super.tearDown();
  }
  
  protected String getTestFile() {
    return "testMRConf.xml";
  }
  
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("JOBTRACKER", jobtracker);
    expCmd = super.expandCommand(cmd);
    return expCmd;
  }
  
  protected Result execute(TestCmd cmd) throws Exception {
    if(cmd.getType() == TestCmd.CommandType.MRADMIN) {
      return cmdExecutor.executeCommand(cmd.getCmd());
    }
    else throw new Exception("Unknow type of Test command:"+ cmd.getType());
  }
  
  public static class MRCmdExecutor extends CommandExecutor {
    private String jobtracker = null;
    public MRCmdExecutor(String jobtracker) {
      this.jobtracker = jobtracker;
    }
    protected void execute(final String cmd) throws Exception{
      MRAdmin mradmin = new MRAdmin();
      String[] args = getCommandAsArgs(cmd, "JOBTRACKER", jobtracker);
      ToolRunner.run(mradmin, args);
    }
  }
}
