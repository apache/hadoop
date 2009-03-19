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
import junit.framework.TestCase;
import org.apache.hadoop.hdfs.TestHDFSServerPorts;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;

/**
 * This test checks correctness of port usage by mapreduce components:
 * JobTracker, and TaskTracker.
 * 
 * The correct behavior is:<br> 
 * - when a specific port is provided the server must either start on that port 
 * or fail by throwing {@link java.net.BindException}.<br>
 * - if the port = 0 (ephemeral) then the server should choose 
 * a free port and start on it.
 */
public class TestMRServerPorts extends TestCase {
  TestHDFSServerPorts hdfs = new TestHDFSServerPorts();
  private static final String STARTED_UNEXPECTEDLY
          = "the Job tracker should not have started";
  private static final String FAILED_TO_START = "The Job tracker did not start";

  // Runs the JT in a separate thread
  private static class JTRunner extends Thread {
    JobTracker jt;
    void setJobTracker(JobTracker jt) {
      this.jt = jt;
    }

    public void run() {
      if (jt != null) {
        try {
          jt.offerService();
        } catch (Exception ioe) {}
      }
    }
  }
  /**
   * Check whether the JobTracker can be started.
   */
  private JobTracker startJobTracker(JobConf conf, JTRunner runner) 
  throws IOException {
    conf.set("mapred.job.tracker", "localhost:0");
    conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");
    JobTracker jt = null;
    try {
      jt = JobTracker.startTracker(conf);
      runner.setJobTracker(jt);
      runner.start();
      conf.set("mapred.job.tracker", "localhost:" + jt.getTrackerPort());
      conf.set("mapred.job.tracker.http.address", 
                            "0.0.0.0:" + jt.getInfoPort());
    } catch(InterruptedException e) {
      throw new IOException(e.getLocalizedMessage());
    }
    return jt;
  }

  /**
   * Check whether the JobTracker can be started.
   */
  private boolean canStartJobTracker(JobConf conf) 
  throws IOException, InterruptedException {
    JobTracker jt = null;
    try {
      jt = JobTracker.startTracker(conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    jt.stopTracker();
    return true;
  }

  /**
   * Check whether the TaskTracker can be started.
   */
  private boolean canStartTaskTracker(JobConf conf) 
  throws IOException, InterruptedException {
    TaskTracker tt = null;
    try {
      tt = new TaskTracker(conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    tt.shutdown();
    return true;
  }

  /**
   * Verify JobTracker port usage.
   */
  public void testJobTrackerPorts() throws Exception {
    NameNode nn = null;
    try {
      nn = hdfs.startNameNode();

      // start job tracker on the same port as name-node
      JobConf conf2 = new JobConf(hdfs.getConfig());
      conf2.set("mapred.job.tracker",
                FileSystem.getDefaultUri(hdfs.getConfig()).toString());
      conf2.set("mapred.job.tracker.http.address",
        TestHDFSServerPorts.NAME_NODE_HTTP_HOST + 0);
      boolean started = canStartJobTracker(conf2);
      assertFalse(STARTED_UNEXPECTEDLY, started); // should fail

      // bind http server to the same port as name-node
      conf2.set("mapred.job.tracker", TestHDFSServerPorts.NAME_NODE_HOST + 0);
      conf2.set("mapred.job.tracker.http.address",
        hdfs.getConfig().get("dfs.http.address"));
      started = canStartJobTracker(conf2);
      assertFalse(STARTED_UNEXPECTEDLY, started); // should fail again

      // both ports are different from the name-node ones
      conf2.set("mapred.job.tracker", TestHDFSServerPorts.NAME_NODE_HOST + 0);
      conf2.set("mapred.job.tracker.http.address",
        TestHDFSServerPorts.NAME_NODE_HTTP_HOST + 0);
      started = canStartJobTracker(conf2);
      assertTrue(FAILED_TO_START, started); // should start now

    } finally {
      hdfs.stopNameNode(nn);
    }
  }

  /**
   * Verify JobTracker port usage.
   */
  public void testTaskTrackerPorts() throws Exception {
    NameNode nn = null;
    JobTracker jt = null;
    JTRunner runner = null;
    try {
      nn = hdfs.startNameNode();

      JobConf conf2 = new JobConf(hdfs.getConfig());
      runner = new JTRunner();
      jt = startJobTracker(conf2, runner);

      // start job tracker on the same port as name-node
      conf2.set("mapred.task.tracker.report.address",
                FileSystem.getDefaultUri(hdfs.getConfig()).toString());
      conf2.set("mapred.task.tracker.http.address",
        TestHDFSServerPorts.NAME_NODE_HTTP_HOST + 0);
      boolean started = canStartTaskTracker(conf2);
      assertFalse(STARTED_UNEXPECTEDLY, started); // should fail

      // bind http server to the same port as name-node
      conf2.set("mapred.task.tracker.report.address",
        TestHDFSServerPorts.NAME_NODE_HOST + 0);
      conf2.set("mapred.task.tracker.http.address",
        hdfs.getConfig().get("dfs.http.address"));
      started = canStartTaskTracker(conf2);
      assertFalse(STARTED_UNEXPECTEDLY, started); // should fail again

      // both ports are different from the name-node ones
      conf2.set("mapred.task.tracker.report.address",
        TestHDFSServerPorts.NAME_NODE_HOST + 0);
      conf2.set("mapred.task.tracker.http.address",
        TestHDFSServerPorts.NAME_NODE_HTTP_HOST + 0);
      started = canStartTaskTracker(conf2);
      assertTrue(FAILED_TO_START, started); // should start now
    } finally {
      if (jt != null) {
        jt.fs.close();
        jt.stopTracker();
        runner.interrupt();
        runner.join();
      }
      hdfs.stopNameNode(nn);
    }
  }
}
