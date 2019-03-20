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
package org.apache.hadoop.tools.dynamometer.workloadgenerator;

import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditCommandParser;
import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditLogDirectParser;
import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditLogHiveTableParser;
import org.apache.hadoop.tools.dynamometer.workloadgenerator.audit.AuditReplayMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/** Tests for {@link WorkloadDriver} and related classes. */
public class TestWorkloadGenerator {

  private Configuration conf;
  private MiniDFSCluster miniCluster;
  private FileSystem dfs;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    conf.setClass(HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS,
        AllowUserImpersonationProvider.class, ImpersonationProvider.class);
    miniCluster = new MiniDFSCluster.Builder(conf).build();
    miniCluster.waitClusterUp();
    dfs = miniCluster.getFileSystem();
    dfs.mkdirs(new Path("/tmp"),
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    dfs.setOwner(new Path("/tmp"), "hdfs", "hdfs");
  }

  @After
  public void tearDown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
      miniCluster = null;
    }
  }

  @Test
  public void testAuditWorkloadDirectParser() throws Exception {
    String workloadInputPath = TestWorkloadGenerator.class.getClassLoader()
        .getResource("audit_trace_direct").toString();
    conf.set(AuditReplayMapper.INPUT_PATH_KEY, workloadInputPath);
    conf.setLong(AuditLogDirectParser.AUDIT_START_TIMESTAMP_KEY, 60 * 1000);
    testAuditWorkload();
  }

  @Test
  public void testAuditWorkloadHiveParser() throws Exception {
    String workloadInputPath = TestWorkloadGenerator.class.getClassLoader()
        .getResource("audit_trace_hive").toString();
    conf.set(AuditReplayMapper.INPUT_PATH_KEY, workloadInputPath);
    conf.setClass(AuditReplayMapper.COMMAND_PARSER_KEY,
        AuditLogHiveTableParser.class, AuditCommandParser.class);
    testAuditWorkload();
  }

  /**
   * {@link ImpersonationProvider} that confirms the user doing the
   * impersonating is the same as the user running the MiniCluster.
   */
  private static class AllowUserImpersonationProvider extends Configured
      implements ImpersonationProvider {
    public void init(String configurationPrefix) {
      // Do nothing
    }

    public void authorize(UserGroupInformation user, String remoteAddress)
        throws AuthorizationException {
      try {
        if (!user.getRealUser().getShortUserName()
            .equals(UserGroupInformation.getCurrentUser().getShortUserName())) {
          throw new AuthorizationException();
        }
      } catch (IOException ioe) {
        throw new AuthorizationException(ioe);
      }
    }
  }

  private void testAuditWorkload() throws Exception {
    long workloadStartTime = System.currentTimeMillis() + 10000;
    Job workloadJob = WorkloadDriver.getJobForSubmission(conf,
        dfs.getUri().toString(), workloadStartTime, AuditReplayMapper.class);
    boolean success = workloadJob.waitForCompletion(true);
    assertTrue("workload job should succeed", success);
    Counters counters = workloadJob.getCounters();
    assertEquals(6,
        counters.findCounter(AuditReplayMapper.REPLAYCOUNTERS.TOTALCOMMANDS)
            .getValue());
    assertEquals(1,
        counters
            .findCounter(AuditReplayMapper.REPLAYCOUNTERS.TOTALINVALIDCOMMANDS)
            .getValue());
    assertTrue(dfs.getFileStatus(new Path("/tmp/test1")).isFile());
    assertTrue(
        dfs.getFileStatus(new Path("/tmp/testDirRenamed")).isDirectory());
    assertFalse(dfs.exists(new Path("/denied")));
  }
}
