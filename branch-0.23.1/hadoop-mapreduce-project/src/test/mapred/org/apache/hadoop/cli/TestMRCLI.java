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

import org.apache.hadoop.cli.util.*;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.authorize.HadoopPolicyProvider;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestMRCLI extends TestHDFSCLI {

  protected MiniMRCluster mrCluster = null;
  protected String jobtracker = null;
  private JobConf mrConf;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
        HadoopPolicyProvider.class, PolicyProvider.class);
    mrConf = new JobConf(conf);
    mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri().toString(), 1, 
                           null, null, mrConf);
    jobtracker = mrCluster.createJobConf().get(JTConfig.JT_IPC_ADDRESS, "local");
  }

  @After
  public void tearDown() throws Exception {
    mrCluster.shutdown();
    super.tearDown();
  }

    @Override
    protected TestConfigFileParser getConfigParser() {
        return new TestConfigFileParserMR();
    }

    protected String getTestFile() {
    return "testMRConf.xml";
  }

  @Override
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("JOBTRACKER", jobtracker);
    expCmd = super.expandCommand(expCmd);
    return expCmd;
  }

  @Override
  protected Result execute(CLICommand cmd) throws Exception {
    if (cmd.getType() instanceof CLICommandMRAdmin)
      return new TestMRCLI.MRCmdExecutor(jobtracker).executeCommand(cmd.getCmd());
    else if (cmd.getType() instanceof CLICommandArchive)
      return new TestMRCLI.ArchiveCmdExecutor(namenode, mrConf).executeCommand(cmd.getCmd());
    else
      return super.execute(cmd);
  }
  
  public static class MRCmdExecutor extends CommandExecutor {
    private String jobtracker = null;
    public MRCmdExecutor(String jobtracker) {
      this.jobtracker = jobtracker;
    }
    @Override
    protected void execute(final String cmd) throws Exception{
      MRAdmin mradmin = new MRAdmin();
      String[] args = getCommandAsArgs(cmd, "JOBTRACKER", jobtracker);
      ToolRunner.run(mradmin, args);
    }

  }
  
  public static class ArchiveCmdExecutor extends CommandExecutor {
    private String namenode = null;
    private JobConf jobConf = null;
    public ArchiveCmdExecutor(String namenode, JobConf jobConf) {
      this.namenode = namenode;
      this.jobConf = jobConf;
    }
    @Override
    protected void execute(final String cmd) throws Exception {
      HadoopArchives archive = new HadoopArchives(jobConf);
      String[] args = getCommandAsArgs(cmd, "NAMENODE", namenode);
      ToolRunner.run(archive, args);
    }
  }

  @Test
  @Ignore
  @Override
  public void testAll () {
    super.testAll();
  }

  class TestConfigFileParserMR extends CLITestHelper.TestConfigFileParser {
    @Override
    public void endElement(String uri, String localName, String qName)
        throws SAXException {
      if (qName.equals("mr-admin-command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmdMR(charString,
              new CLICommandMRAdmin()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmdMR(charString,
              new CLICommandMRAdmin()));
        }
      } else if (qName.equals("archive-command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmdMR(charString,
              new CLICommandArchive()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmdMR(charString,
              new CLICommandArchive()));
        }
      } else {
        super.endElement(uri, localName, qName);
      }
    }
  }
}
