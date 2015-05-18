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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class TestIntegration {
  private static final Log LOG = LogFactory.getLog(TestIntegration.class);

  private static FileSystem fs;

  private static Path listFile;
  private static Path target;
  private static String root;
  private int numListstatusThreads;

  public TestIntegration(int numListstatusThreads) {
    this.numListstatusThreads = numListstatusThreads;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 1 }, { 2 }, { 10 } };
    return Arrays.asList(data);
  }

  private static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    return conf;
  }

  @BeforeClass
  public static void setup() {
    try {
      fs = FileSystem.get(getConf());
      listFile = new Path("target/tmp/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      target = new Path("target/tmp/target").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      root = new Path("target/tmp").makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString();
      TestDistCpUtils.delete(fs, root);
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  @Test(timeout=100000)
  public void testSingleFileMissingTarget() {
    caseSingleFileMissingTarget(false);
    caseSingleFileMissingTarget(true);
  }

  private void caseSingleFileMissingTarget(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1");
      createFiles("singlefile1/file1");

      runTest(listFile, target, false, sync);

      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleFileTargetFile() {
    caseSingleFileTargetFile(false);
    caseSingleFileTargetFile(true);
  }

  private void caseSingleFileTargetFile(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1");
      createFiles("singlefile1/file1", "target");

      runTest(listFile, target, false, sync);

      checkResult(target, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleFileTargetDir() {
    caseSingleFileTargetDir(false);
    caseSingleFileTargetDir(true);
  }

  private void caseSingleFileTargetDir(boolean sync) {

    try {
      addEntries(listFile, "singlefile2/file2");
      createFiles("singlefile2/file2");
      mkdirs(target.toString());

      runTest(listFile, target, true, sync);

      checkResult(target, 1, "file2");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleDirTargetMissing() {
    caseSingleDirTargetMissing(false);
    caseSingleDirTargetMissing(true);
  }

  private void caseSingleDirTargetMissing(boolean sync) {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, false, sync);

      checkResult(target, 1, "dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleDirTargetPresent() {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");
      mkdirs(target.toString());

      runTest(listFile, target, true, false);

      checkResult(target, 1, "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testUpdateSingleDirTargetPresent() {

    try {
      addEntries(listFile, "Usingledir");
      mkdirs(root + "/Usingledir/Udir1");
      mkdirs(target.toString());

      runTest(listFile, target, true, true);

      checkResult(target, 1, "Udir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testMultiFileTargetPresent() {
    caseMultiFileTargetPresent(false);
    caseMultiFileTargetPresent(true);
  }

  private void caseMultiFileTargetPresent(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(target.toString());

      runTest(listFile, target, true, sync);

      checkResult(target, 3, "file3", "file4", "file5");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testMultiFileTargetMissing() {
    caseMultiFileTargetMissing(false);
    caseMultiFileTargetMissing(true);
  }

  private void caseMultiFileTargetMissing(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");

      runTest(listFile, target, false, sync);

      checkResult(target, 3, "file3", "file4", "file5");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testMultiDirTargetPresent() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(target.toString(), root + "/singledir/dir1");

      runTest(listFile, target, true, false);

      checkResult(target, 2, "multifile/file3", "multifile/file4", "multifile/file5", "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testUpdateMultiDirTargetPresent() {

    try {
      addEntries(listFile, "Umultifile", "Usingledir");
      createFiles("Umultifile/Ufile3", "Umultifile/Ufile4", "Umultifile/Ufile5");
      mkdirs(target.toString(), root + "/Usingledir/Udir1");

      runTest(listFile, target, true, true);

      checkResult(target, 4, "Ufile3", "Ufile4", "Ufile5", "Udir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testMultiDirTargetMissing() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, false, false);

      checkResult(target, 2, "multifile/file3", "multifile/file4",
          "multifile/file5", "singledir/dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testUpdateMultiDirTargetMissing() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(root + "/singledir/dir1");

      runTest(listFile, target, false, true);

      checkResult(target, 4, "file3", "file4", "file5", "dir1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }
  
  @Test(timeout=100000)
  public void testDeleteMissingInDestination() {
    
    try {
      addEntries(listFile, "srcdir");
      createFiles("srcdir/file1", "dstdir/file1", "dstdir/file2");
      
      Path target = new Path(root + "/dstdir");
      runTest(listFile, target, false, true, true, false);
      
      checkResult(target, 1, "file1");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }
  
  @Test(timeout=100000)
  public void testOverwrite() {
    byte[] contents1 = "contents1".getBytes();
    byte[] contents2 = "contents2".getBytes();
    Assert.assertEquals(contents1.length, contents2.length);
    
    try {
      addEntries(listFile, "srcdir");
      createWithContents("srcdir/file1", contents1);
      createWithContents("dstdir/file1", contents2);
      
      Path target = new Path(root + "/dstdir");
      runTest(listFile, target, false, false, false, true);
      
      checkResult(target, 1, "file1");
      
      // make sure dstdir/file1 has been overwritten with the contents
      // of srcdir/file1
      FSDataInputStream is = fs.open(new Path(root + "/dstdir/file1"));
      byte[] dstContents = new byte[contents1.length];
      is.readFully(dstContents);
      is.close();
      Assert.assertArrayEquals(contents1, dstContents);
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testGlobTargetMissingSingleLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
                                fs.getWorkingDirectory());
      addEntries(listFile, "*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir/dir2/file6");

      runTest(listFile, target, false, false);

      checkResult(target, 2, "multifile/file3", "multifile/file4", "multifile/file5",
          "singledir/dir2/file6");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testUpdateGlobTargetMissingSingleLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
                                  fs.getWorkingDirectory());
      addEntries(listFile, "*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir/dir2/file6");

      runTest(listFile, target, false, true);

      checkResult(target, 4, "file3", "file4", "file5", "dir2/file6");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testGlobTargetMissingMultiLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      addEntries(listFile, "*/*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir1/dir3/file7", "singledir1/dir3/file8",
          "singledir1/dir3/file9");

      runTest(listFile, target, false, false);

      checkResult(target, 4, "file3", "file4", "file5",
          "dir3/file7", "dir3/file8", "dir3/file9");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testUpdateGlobTargetMissingMultiLevel() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      addEntries(listFile, "*/*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir1/dir3/file7", "singledir1/dir3/file8",
          "singledir1/dir3/file9");

      runTest(listFile, target, false, true);

      checkResult(target, 6, "file3", "file4", "file5",
          "file7", "file8", "file9");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }
  
  @Test(timeout=100000)
  public void testCleanup() {
    try {
      Path sourcePath = new Path("noscheme:///file");
      List<Path> sources = new ArrayList<Path>();
      sources.add(sourcePath);

      DistCpOptions options = new DistCpOptions(sources, target);

      Configuration conf = getConf();
      Path stagingDir = JobSubmissionFiles.getStagingDir(
              new Cluster(conf), conf);
      stagingDir.getFileSystem(conf).mkdirs(stagingDir);

      try {
        new DistCp(conf, options).execute();
      } catch (Throwable t) {
        Assert.assertEquals(stagingDir.getFileSystem(conf).
            listStatus(stagingDir).length, 0);
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("testCleanup failed " + e.getMessage());
    }
  }
  
  private void addEntries(Path listFile, String... entries) throws IOException {
    OutputStream out = fs.create(listFile);
    try {
      for (String entry : entries){
        out.write((root + "/" + entry).getBytes());
        out.write("\n".getBytes());
      }
    } finally {
      out.close();
    }
  }

  private void createFiles(String... entries) throws IOException {
    for (String entry : entries){
      OutputStream out = fs.create(new Path(root + "/" + entry));
      try {
        out.write((root + "/" + entry).getBytes());
        out.write("\n".getBytes());
      } finally {
        out.close();
      }
    }
  }
  
  private void createWithContents(String entry, byte[] contents) throws IOException {
    OutputStream out = fs.create(new Path(root + "/" + entry));
    try {
      out.write(contents);
    } finally {
      out.close();
    }
  }

  private void mkdirs(String... entries) throws IOException {
    for (String entry : entries){
      fs.mkdirs(new Path(entry));
    }
  }
    
  private void runTest(Path listFile, Path target, boolean targetExists,
      boolean sync) throws IOException {
    runTest(listFile, target, targetExists, sync, false, false);
  }
  
  private void runTest(Path listFile, Path target, boolean targetExists, 
      boolean sync, boolean delete,
      boolean overwrite) throws IOException {
    DistCpOptions options = new DistCpOptions(listFile, target);
    options.setSyncFolder(sync);
    options.setDeleteMissing(delete);
    options.setOverwrite(overwrite);
    options.setTargetPathExists(targetExists);
    options.setNumListstatusThreads(numListstatusThreads);
    try {
      new DistCp(getConf(), options).execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw new IOException(e);
    }
  }

  private void checkResult(Path target, int count, String... relPaths) throws IOException {
    Assert.assertEquals(count, fs.listStatus(target).length);
    if (relPaths == null || relPaths.length == 0) {
      Assert.assertTrue(target.toString(), fs.exists(target));
      return;
    }
    for (String relPath : relPaths) {
      Assert.assertTrue(new Path(target, relPath).toString(), fs.exists(new Path(target, relPath)));
    }
  }

}
