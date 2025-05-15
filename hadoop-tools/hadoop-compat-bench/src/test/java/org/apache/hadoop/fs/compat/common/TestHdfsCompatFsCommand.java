/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.fs.compat.HdfsCompatTool;
import org.apache.hadoop.fs.compat.hdfs.HdfsCompatMiniCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.compat.cases.HdfsCompatAclTestCases;
import org.apache.hadoop.fs.compat.cases.HdfsCompatMkdirTestCases;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class TestHdfsCompatFsCommand {
  @Test
  public void testDfsCompatibility() throws Exception {
    final String suite = "ALL";
    HdfsCompatMiniCluster cluster = null;
    try {
      cluster = new HdfsCompatMiniCluster();
      cluster.start();
      final String uri = cluster.getUri() + "/tmp";
      final Configuration conf = cluster.getConf();

      HdfsCompatCommand cmd = new TestCommand(uri, suite, conf);
      cmd.initialize();
      HdfsCompatReport report = cmd.apply();
      assertEquals(7, report.getPassedCase().size());
      assertEquals(0, report.getFailedCase().size());
      show(conf, report);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testLocalFsCompatibility() throws Exception {
    final String uri = "file:///tmp/";
    final String suite = "ALL";
    final Configuration conf = new Configuration();
    HdfsCompatCommand cmd = new TestCommand(uri, suite, conf);
    cmd.initialize();
    HdfsCompatReport report = cmd.apply();
    assertEquals(1, report.getPassedCase().size());
    assertEquals(6, report.getFailedCase().size());
    show(conf, report);
    cleanup(cmd, conf);
  }

  @Test
  public void testFsCompatibilityWithSuite() throws Exception {
    final String uri = "file:///tmp/";
    final String suite = "acl";
    final Configuration conf = new Configuration();
    HdfsCompatCommand cmd = new TestCommand(uri, suite, conf);
    cmd.initialize();
    HdfsCompatReport report = cmd.apply();
    assertEquals(0, report.getPassedCase().size());
    assertEquals(6, report.getFailedCase().size());
    show(conf, report);
    cleanup(cmd, conf);
  }

  private void show(Configuration conf, HdfsCompatReport report) throws IOException {
    new HdfsCompatTool(conf).printReport(report, System.out);
  }

  private void cleanup(HdfsCompatCommand cmd, Configuration conf) throws Exception {
    Path basePath = ((TestCommand) cmd).getBasePath();
    FileSystem fs = basePath.getFileSystem(conf);
    fs.delete(basePath, true);
  }

  private static final class TestCommand extends HdfsCompatCommand {
    private TestCommand(String uri, String suiteName, Configuration conf) {
      super(uri, suiteName, conf);
    }

    @Override
    protected Map<String, HdfsCompatSuite> getDefaultSuites() {
      Map<String, HdfsCompatSuite> defaultSuites = new HashMap<>();
      defaultSuites.put("all", new AllTestSuite());
      defaultSuites.put("mkdir", new MkdirTestSuite());
      defaultSuites.put("acl", new AclTestSuite());
      return defaultSuites;
    }

    private Path getBasePath() throws ReflectiveOperationException {
      Field apiField = HdfsCompatCommand.class.getDeclaredField("api");
      apiField.setAccessible(true);
      HdfsCompatApiScope api = (HdfsCompatApiScope) apiField.get(this);
      Field envField = api.getClass().getDeclaredField("env");
      envField.setAccessible(true);
      HdfsCompatEnvironment env = (HdfsCompatEnvironment) envField.get(api);
      return env.getBase();
    }
  }

  private static class AllTestSuite implements HdfsCompatSuite {
    @Override
    public String getSuiteName() {
      return "All (Test)";
    }

    @Override
    public Class<? extends AbstractHdfsCompatCase>[] getApiCases() {
      return new Class[]{
          HdfsCompatMkdirTestCases.class,
          HdfsCompatAclTestCases.class,
      };
    }

    @Override
    public String[] getShellCases() {
      return new String[0];
    }
  }

  private static class MkdirTestSuite implements HdfsCompatSuite {
    @Override
    public String getSuiteName() {
      return "Mkdir";
    }

    @Override
    public Class<? extends AbstractHdfsCompatCase>[] getApiCases() {
      return new Class[]{
          HdfsCompatMkdirTestCases.class,
      };
    }

    @Override
    public String[] getShellCases() {
      return new String[0];
    }
  }

  private static class AclTestSuite implements HdfsCompatSuite {
    @Override
    public String getSuiteName() {
      return "ACL";
    }

    @Override
    public Class<? extends AbstractHdfsCompatCase>[] getApiCases() {
      return new Class[]{
          HdfsCompatAclTestCases.class,
      };
    }

    @Override
    public String[] getShellCases() {
      return new String[0];
    }
  }
}