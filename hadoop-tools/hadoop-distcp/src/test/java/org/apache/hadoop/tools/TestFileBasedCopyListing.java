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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.security.Credentials;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class TestFileBasedCopyListing {
  private static final Log LOG = LogFactory.getLog(TestFileBasedCopyListing.class);

  private static final Credentials CREDENTIALS = new Credentials();

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void create() throws IOException {
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).format(true)
                                                .build();
    fs = cluster.getFileSystem();
    buildExpectedValuesMap();
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static Map<String, String> map = new HashMap<String, String>();

  private static void buildExpectedValuesMap() {
    map.put("/file1", "/tmp/singlefile1/file1");
    map.put("/file2", "/tmp/singlefile2/file2");
    map.put("/file3", "/tmp/multifile/file3");
    map.put("/file4", "/tmp/multifile/file4");
    map.put("/file5", "/tmp/multifile/file5");
    map.put("/multifile/file3", "/tmp/multifile/file3");
    map.put("/multifile/file4", "/tmp/multifile/file4");
    map.put("/multifile/file5", "/tmp/multifile/file5");
    map.put("/Ufile3", "/tmp/Umultifile/Ufile3");
    map.put("/Ufile4", "/tmp/Umultifile/Ufile4");
    map.put("/Ufile5", "/tmp/Umultifile/Ufile5");
    map.put("/dir1", "/tmp/singledir/dir1");
    map.put("/singledir/dir1", "/tmp/singledir/dir1");
    map.put("/dir2", "/tmp/singledir/dir2");
    map.put("/singledir/dir2", "/tmp/singledir/dir2");
    map.put("/Udir1", "/tmp/Usingledir/Udir1");
    map.put("/Udir2", "/tmp/Usingledir/Udir2");
    map.put("/dir2/file6", "/tmp/singledir/dir2/file6");
    map.put("/singledir/dir2/file6", "/tmp/singledir/dir2/file6");
    map.put("/file7", "/tmp/singledir1/dir3/file7");
    map.put("/file8", "/tmp/singledir1/dir3/file8");
    map.put("/file9", "/tmp/singledir1/dir3/file9");
    map.put("/dir3/file7", "/tmp/singledir1/dir3/file7");
    map.put("/dir3/file8", "/tmp/singledir1/dir3/file8");
    map.put("/dir3/file9", "/tmp/singledir1/dir3/file9");
    map.put("/Ufile7", "/tmp/Usingledir1/Udir3/Ufile7");
    map.put("/Ufile8", "/tmp/Usingledir1/Udir3/Ufile8");
    map.put("/Ufile9", "/tmp/Usingledir1/Udir3/Ufile9");
  }

  @Test
  public void testSingleFileMissingTarget() {
    caseSingleFileMissingTarget(false);
    caseSingleFileMissingTarget(true);
  }

  private void caseSingleFileMissingTarget(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/singlefile1/file1");
      createFiles("/tmp/singlefile1/file1");

      runTest(listFile, target, false, sync);

      checkResult(listFile, 0);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testSingleFileTargetFile() {
    caseSingleFileTargetFile(false);
    caseSingleFileTargetFile(true);
  }

  private void caseSingleFileTargetFile(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/singlefile1/file1");
      createFiles("/tmp/singlefile1/file1", target.toString());

      runTest(listFile, target, false, sync);

      checkResult(listFile, 0);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testSingleFileTargetDir() {
    caseSingleFileTargetDir(false);
    caseSingleFileTargetDir(true);
  }

  private void caseSingleFileTargetDir(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/singlefile2/file2");
      createFiles("/tmp/singlefile2/file2");
      mkdirs(target.toString());

      runTest(listFile, target, true, sync);

      checkResult(listFile, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testSingleDirTargetMissing() {
    caseSingleDirTargetMissing(false);
    caseSingleDirTargetMissing(true);
  }

  private void caseSingleDirTargetMissing(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/singledir");
      mkdirs("/tmp/singledir/dir1");

      runTest(listFile, target, false, sync);

      checkResult(listFile, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testSingleDirTargetPresent() {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/singledir");
      mkdirs("/tmp/singledir/dir1");
      mkdirs(target.toString());

      runTest(listFile, target, true);

      checkResult(listFile, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testUpdateSingleDirTargetPresent() {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/Usingledir");
      mkdirs("/tmp/Usingledir/Udir1");
      mkdirs(target.toString());

      runTest(listFile, target, true, true);

      checkResult(listFile, 1);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testMultiFileTargetPresent() {
    caseMultiFileTargetPresent(false);
    caseMultiFileTargetPresent(true);
  }

  private void caseMultiFileTargetPresent(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      mkdirs(target.toString());

      runTest(listFile, target, true, sync);

      checkResult(listFile, 3);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testMultiFileTargetMissing() {
    caseMultiFileTargetMissing(false);
    caseMultiFileTargetMissing(true);
  }

  private void caseMultiFileTargetMissing(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");

      runTest(listFile, target, false, sync);

      checkResult(listFile, 3);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testMultiDirTargetPresent() {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/multifile", "/tmp/singledir");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      mkdirs(target.toString(), "/tmp/singledir/dir1");

      runTest(listFile, target, true);

      checkResult(listFile, 4);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testUpdateMultiDirTargetPresent() {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/Umultifile", "/tmp/Usingledir");
      createFiles("/tmp/Umultifile/Ufile3", "/tmp/Umultifile/Ufile4", "/tmp/Umultifile/Ufile5");
      mkdirs(target.toString(), "/tmp/Usingledir/Udir1");

      runTest(listFile, target, true);

      checkResult(listFile, 4);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testMultiDirTargetMissing() {
    caseMultiDirTargetMissing(false);
    caseMultiDirTargetMissing(true);
  }

  private void caseMultiDirTargetMissing(boolean sync) {

    try {
      Path listFile = new Path("/tmp/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/multifile", "/tmp/singledir");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      mkdirs("/tmp/singledir/dir1");

      runTest(listFile, target, sync);

      checkResult(listFile, 4);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test
  public void testGlobTargetMissingSingleLevel() {
    caseGlobTargetMissingSingleLevel(false);
    caseGlobTargetMissingSingleLevel(true);
  }

  private void caseGlobTargetMissingSingleLevel(boolean sync) {

    try {
      Path listFile = new Path("/tmp1/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/*");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      createFiles("/tmp/singledir/dir2/file6");

      runTest(listFile, target, sync);

      checkResult(listFile, 5);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
      TestDistCpUtils.delete(fs, "/tmp1");
    }
  }

  @Test
  public void testGlobTargetMissingMultiLevel() {
    caseGlobTargetMissingMultiLevel(false);
    caseGlobTargetMissingMultiLevel(true);
  }

  private void caseGlobTargetMissingMultiLevel(boolean sync) {

    try {
      Path listFile = new Path("/tmp1/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/*/*");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      createFiles("/tmp/singledir1/dir3/file7", "/tmp/singledir1/dir3/file8",
          "/tmp/singledir1/dir3/file9");

      runTest(listFile, target, sync);

      checkResult(listFile, 6);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
      TestDistCpUtils.delete(fs, "/tmp1");
    }
  }

  @Test
  public void testGlobTargetDirMultiLevel() {

    try {
      Path listFile = new Path("/tmp1/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/*/*");
      createFiles("/tmp/multifile/file3", "/tmp/multifile/file4", "/tmp/multifile/file5");
      createFiles("/tmp/singledir1/dir3/file7", "/tmp/singledir1/dir3/file8",
          "/tmp/singledir1/dir3/file9");
      mkdirs(target.toString());

      runTest(listFile, target, true);

      checkResult(listFile, 6);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
      TestDistCpUtils.delete(fs, "/tmp1");
    }
  }

  @Test
  public void testUpdateGlobTargetDirMultiLevel() {

    try {
      Path listFile = new Path("/tmp1/listing");
      Path target = new Path("/tmp/target");

      addEntries(listFile, "/tmp/*/*");
      createFiles("/tmp/Umultifile/Ufile3", "/tmp/Umultifile/Ufile4", "/tmp/Umultifile/Ufile5");
      createFiles("/tmp/Usingledir1/Udir3/Ufile7", "/tmp/Usingledir1/Udir3/Ufile8",
          "/tmp/Usingledir1/Udir3/Ufile9");
      mkdirs(target.toString());

      runTest(listFile, target, true);

      checkResult(listFile, 6);
    } catch (IOException e) {
      LOG.error("Exception encountered while testing build listing", e);
      Assert.fail("build listing failure");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
      TestDistCpUtils.delete(fs, "/tmp1");
    }
  }

  private void addEntries(Path listFile, String... entries) throws IOException {
    OutputStream out = fs.create(listFile);
    try {
      for (String entry : entries){
        out.write(entry.getBytes());
        out.write("\n".getBytes());
      }
    } finally {
      out.close();
    }
  }

  private void createFiles(String... entries) throws IOException {
    for (String entry : entries){
      OutputStream out = fs.create(new Path(entry));
      try {
        out.write(entry.getBytes());
        out.write("\n".getBytes());
      } finally {
        out.close();
      }
    }
  }

  private void mkdirs(String... entries) throws IOException {
    for (String entry : entries){
      fs.mkdirs(new Path(entry));
    }
  }

  private void runTest(Path listFile, Path target,
      boolean targetExists) throws IOException {
    runTest(listFile, target, targetExists, true);
  }

  private void runTest(Path listFile, Path target, boolean targetExists,
      boolean sync) throws IOException {
    CopyListing listing = new FileBasedCopyListing(config, CREDENTIALS);
    DistCpOptions options = new DistCpOptions(listFile, target);
    options.setSyncFolder(sync);
    options.setTargetPathExists(targetExists);
    listing.buildListing(listFile, options);
  }

  private void checkResult(Path listFile, int count) throws IOException {
    if (count == 0) {
      return;
    }

    int recCount = 0;
    SequenceFile.Reader reader = new SequenceFile.Reader(config,
                                            SequenceFile.Reader.file(listFile));
    try {
      Text relPath = new Text();
      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      while (reader.next(relPath, fileStatus)) {
        if (fileStatus.isDirectory() && relPath.toString().equals("")) {
          // ignore root with empty relPath, which is an entry to be 
          // used for preserving root attributes etc.
          continue;
        }
        Assert.assertEquals(fileStatus.getPath().toUri().getPath(), map.get(relPath.toString()));
        recCount++;
      }
    } finally {
      IOUtils.closeStream(reader);
    }
    Assert.assertEquals(recCount, count);
  }

}
