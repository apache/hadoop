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
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Permission;

import static org.mockito.Mockito.*;

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

  @Before
  public void setup() {

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    try {
      fs = FileSystem.get(getConf());
      root = new Path("target/tmp").makeQualified(fs.getUri(),
          fs.getWorkingDirectory()).toString();
      TestDistCpUtils.delete(fs, root);
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  @After
  public void tearDown() {
    System.setSecurityManager(securityManager);
  }
/**
 * test methods run end execute of DistCp class. silple copy file
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
      String[] arg = { soure.toString(), target.toString() };

      distcp.run(arg);
      Assert.assertTrue(fs.exists(target));

  
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
      Assert.fail();

    } catch (ExitException t) {
      Assert.assertTrue(fs.exists(target));
      Assert.assertEquals(t.status, 0);
      Assert.assertEquals(
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
    Mockito.when(distcp.getConf()).thenReturn(conf);
    Mockito.when(distcp.execute()).thenReturn(job);
    Mockito.when(distcp.run(Mockito.any())).thenCallRealMethod();
    String[] arg = { soure.toString(), target.toString() };

    distcp.run(arg);
    Mockito.verify(job, times(1)).close();
  }


  private SecurityManager securityManager;

  protected static class ExitException extends SecurityException {
    private static final long serialVersionUID = -1982617086752946683L;
    public final int status;

    public ExitException(int status) {
      super("There is no escape!");
      this.status = status;
    }
  }

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }
}
