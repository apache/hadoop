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

package org.apache.hadoop.tools;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestExternalCall {

  private static final Logger LOG = LoggerFactory.getLogger(TestExternalCall.class);

  private static FileSystem fs;

  private static String root;

  private static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    return conf;
  }

  @BeforeEach
  public void setup() {
    ExitUtil.disableSystemExit();
    ExitUtil.disableSystemHalt();
    ExitUtil.resetFirstExitException();
    ExitUtil.resetFirstHaltException();

    try {
      fs = FileSystem.get(getConf());
      root = new Path("target/tmp").makeQualified(fs.getUri(),
          fs.getWorkingDirectory()).toString();
      TestDistCpUtils.delete(fs, root);
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  @AfterEach
  public void tearDown() {
    ExitUtil.resetFirstExitException();
    ExitUtil.resetFirstHaltException();
  }

/**
 * test methods run end execute of DistCp class. simple copy file
 * @throws Exception 
 */
  @Test
  public void testCleanup() throws Exception {

    Configuration conf = getConf();

    Path stagingDir = JobSubmissionFiles.getStagingDir(new Cluster(conf),
        conf);
    stagingDir.getFileSystem(conf).mkdirs(stagingDir);
    Path soure = createFile("tmp.txt");
    Path target = createFile("target.txt");

    DistCp distcp = new DistCp(conf, null);
    String[] arg = {soure.toString(), target.toString()};

    distcp.run(arg);
    assertTrue(fs.exists(target));

  }

  private Path createFile(String fname) throws IOException {
    Path result = new Path(root + "/" + fname);
    OutputStream out = fs.create(result);
    try {
      out.write((root + "/" + fname).getBytes());
      out.write("\n".getBytes());
    } finally {
      out.close();
    }
    return result;
  }

  /**
   * test main method of DistCp. Method should to call System.exit().
   * 
   */
  @Test
  public void testCleanupTestViaToolRunner() throws IOException, InterruptedException {

    Configuration conf = getConf();

    Path stagingDir = JobSubmissionFiles.getStagingDir(new Cluster(conf), conf);
    stagingDir.getFileSystem(conf).mkdirs(stagingDir);
   
    Path soure = createFile("tmp.txt");
    Path target = createFile("target.txt");
    try {

      String[] arg = {target.toString(),soure.toString()};
      DistCp.main(arg);
      fail();

    } catch (ExitUtil.ExitException t) {
      assertTrue(fs.exists(target));
      assertEquals(t.status, 0);
      assertEquals(
          stagingDir.getFileSystem(conf).listStatus(stagingDir).length, 0);
    }

  }

  /**
   * test methods run end execute of DistCp class. distcp job should be cleaned up after completion
   * @throws Exception
   */
  @Test
  public void testCleanupOfJob() throws Exception {

    Configuration conf = getConf();

    Path stagingDir = JobSubmissionFiles.getStagingDir(new Cluster(conf),
      conf);
    stagingDir.getFileSystem(conf).mkdirs(stagingDir);
    Path soure = createFile("tmp.txt");
    Path target = createFile("target.txt");

    DistCp distcp = mock(DistCp.class);
    Job job = spy(Job.class);
    when(distcp.getConf()).thenReturn(conf);
    when(distcp.createAndSubmitJob()).thenReturn(job);
    when(distcp.execute()).thenCallRealMethod();
    when(distcp.execute(anyBoolean())).thenCallRealMethod();
    doReturn(true).when(job).waitForCompletion(anyBoolean());
    when(distcp.run(any())).thenCallRealMethod();
    String[] arg = { soure.toString(), target.toString() };

    distcp.run(arg);
    verify(job, times(1)).close();
  }


}
