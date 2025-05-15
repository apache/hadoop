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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.compat.HdfsCompatTool;
import org.apache.hadoop.fs.compat.hdfs.HdfsCompatMiniCluster;
import org.apache.hadoop.fs.compat.hdfs.HdfsCompatTestCommand;
import org.apache.hadoop.fs.compat.hdfs.HdfsCompatTestShellScope;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestHdfsCompatShellCommand {
  private HdfsCompatMiniCluster cluster;

  @BeforeEach
  public void runCluster() throws IOException {
    this.cluster = new HdfsCompatMiniCluster();
    this.cluster.start();
  }

  @AfterEach
  public void shutdownCluster() {
    this.cluster.shutdown();
    this.cluster = null;
  }

  @Test
  public void testDfsCompatibility() throws Exception {
    final String uri = cluster.getUri() + "/tmp";
    final Configuration conf = cluster.getConf();
    HdfsCompatCommand cmd = new TestCommand(uri, conf);
    cmd.initialize();
    HdfsCompatReport report = cmd.apply();
    assertEquals(3, report.getPassedCase().size());
    assertEquals(0, report.getFailedCase().size());
    show(conf, report);
  }

  @Test
  public void testSkipCompatibility() throws Exception {
    final String uri = cluster.getUri() + "/tmp";
    final Configuration conf = cluster.getConf();
    HdfsCompatCommand cmd = new TestSkipCommand(uri, conf);
    cmd.initialize();
    HdfsCompatReport report = cmd.apply();
    assertEquals(2, report.getPassedCase().size());
    assertEquals(0, report.getFailedCase().size());
    show(conf, report);
  }

  private void show(Configuration conf, HdfsCompatReport report) throws IOException {
    new HdfsCompatTool(conf).printReport(report, System.out);
  }

  private static final class TestCommand extends HdfsCompatTestCommand {
    private TestCommand(String uri, Configuration conf) {
      super(uri, "shell", conf);
    }

    @Override
    protected HdfsCompatShellScope getShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
      return new TestShellScope(env, suite);
    }
  }

  private static final class TestSkipCommand extends HdfsCompatTestCommand {
    private TestSkipCommand(String uri, Configuration conf) {
      super(uri, "shell", conf);
    }

    @Override
    protected HdfsCompatShellScope getShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
      return new TestShellScopeForSkip(env, suite);
    }
  }

  private static final class TestShellScope extends HdfsCompatTestShellScope {
    private TestShellScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
      super(env, suite);
    }

    @Override
    protected void replace(File scriptDir) throws IOException {
      File casesDir = new File(scriptDir, "cases");
      FileUtils.deleteDirectory(casesDir);
      Files.createDirectories(casesDir.toPath());
      copyResource("/test-case-simple.t", new File(casesDir, "test-case-simple.t"));
    }
  }

  private static final class TestShellScopeForSkip extends HdfsCompatTestShellScope {
    private TestShellScopeForSkip(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
      super(env, suite);
    }

    @Override
    protected void replace(File scriptDir) throws IOException {
      File casesDir = new File(scriptDir, "cases");
      FileUtils.deleteDirectory(casesDir);
      Files.createDirectories(casesDir.toPath());
      copyResource("/test-case-skip.t", new File(casesDir, "test-case-skip.t"));
    }
  }
}