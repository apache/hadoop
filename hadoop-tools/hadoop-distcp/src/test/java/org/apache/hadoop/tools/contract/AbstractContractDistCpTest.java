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

package org.apache.hadoop.tools.contract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Contract test suite covering a file system's integration with DistCp.  The
 * tests coordinate two file system instances: one "local", which is the local
 * file system, and the other "remote", which is the file system implementation
 * under test.  The tests in the suite cover both copying from local to remote
 * (e.g. a backup use case) and copying from remote to local (e.g. a restore use
 * case).
 */
public abstract class AbstractContractDistCpTest
    extends AbstractFSContractTestBase {

  @Rule
  public TestName testName = new TestName();

  private Configuration conf;
  private FileSystem localFS, remoteFS;
  private Path localDir, remoteDir;

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = new Configuration();
    newConf.set("mapred.job.tracker", "local");
    return newConf;
  }

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
    conf = getContract().getConf();
    localFS = FileSystem.getLocal(conf);
    remoteFS = getFileSystem();
    // Test paths are isolated by concrete subclass name and test method name.
    // All paths are fully qualified including scheme (not taking advantage of
    // default file system), so if something fails, the messages will make it
    // clear which paths are local and which paths are remote.
    Path testSubDir = new Path(getClass().getSimpleName(),
        testName.getMethodName());
    localDir = localFS.makeQualified(new Path(new Path(
        GenericTestUtils.getTestDir().toURI()), testSubDir));
    mkdirs(localFS, localDir);
    remoteDir = remoteFS.makeQualified(
        new Path(getContract().getTestPath(), testSubDir));
    mkdirs(remoteFS, remoteDir);
  }

  @Test
  public void deepDirectoryStructureToRemote() throws Exception {
    describe("copy a deep directory structure from local to remote");
    deepDirectoryStructure(localFS, localDir, remoteFS, remoteDir);
  }

  @Test
  public void largeFilesToRemote() throws Exception {
    describe("copy multiple large files from local to remote");
    largeFiles(localFS, localDir, remoteFS, remoteDir);
  }

  @Test
  public void deepDirectoryStructureFromRemote() throws Exception {
    describe("copy a deep directory structure from remote to local");
    deepDirectoryStructure(remoteFS, remoteDir, localFS, localDir);
  }

  @Test
  public void largeFilesFromRemote() throws Exception {
    describe("copy multiple large files from remote to local");
    largeFiles(remoteFS, remoteDir, localFS, localDir);
  }

  /**
   * Executes a test using a file system sub-tree with multiple nesting levels.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @throws Exception if there is a failure
   */
  private void deepDirectoryStructure(FileSystem srcFS, Path srcDir,
      FileSystem dstFS, Path dstDir) throws Exception {
    Path inputDir = new Path(srcDir, "inputDir");
    Path inputSubDir1 = new Path(inputDir, "subDir1");
    Path inputSubDir2 = new Path(inputDir, "subDir2/subDir3");
    Path inputFile1 = new Path(inputDir, "file1");
    Path inputFile2 = new Path(inputSubDir1, "file2");
    Path inputFile3 = new Path(inputSubDir2, "file3");
    mkdirs(srcFS, inputSubDir1);
    mkdirs(srcFS, inputSubDir2);
    byte[] data1 = dataset(100, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset(200, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    byte[] data3 = dataset(300, 53, 63);
    createFile(srcFS, inputFile3, true, data3);
    Path target = new Path(dstDir, "outputDir");
    runDistCp(inputDir, target);
    ContractTestUtils.assertIsDirectory(dstFS, target);
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS,
        new Path(target, "inputDir/subDir1/file2"), data2);
    verifyFileContents(dstFS,
        new Path(target, "inputDir/subDir2/subDir3/file3"), data3);
  }

  /**
   * Executes a test using multiple large files.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @throws Exception if there is a failure
   */
  private void largeFiles(FileSystem srcFS, Path srcDir, FileSystem dstFS,
      Path dstDir) throws Exception {
    Path inputDir = new Path(srcDir, "inputDir");
    Path inputFile1 = new Path(inputDir, "file1");
    Path inputFile2 = new Path(inputDir, "file2");
    Path inputFile3 = new Path(inputDir, "file3");
    mkdirs(srcFS, inputDir);
    int fileSizeKb = conf.getInt("scale.test.distcp.file.size.kb", 10 * 1024);
    int fileSizeMb = fileSizeKb / 1024;
    getLog().info("{} with file size {}", testName.getMethodName(), fileSizeMb);
    byte[] data1 = dataset((fileSizeMb + 1) * 1024 * 1024, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset((fileSizeMb + 2) * 1024 * 1024, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    byte[] data3 = dataset((fileSizeMb + 3) * 1024 * 1024, 53, 63);
    createFile(srcFS, inputFile3, true, data3);
    Path target = new Path(dstDir, "outputDir");
    runDistCp(inputDir, target);
    ContractTestUtils.assertIsDirectory(dstFS, target);
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS, new Path(target, "inputDir/file2"), data2);
    verifyFileContents(dstFS, new Path(target, "inputDir/file3"), data3);
  }

  /**
   * Executes DistCp and asserts that the job finished successfully.
   *
   * @param src source path
   * @param dst destination path
   * @throws Exception if there is a failure
   */
  private void runDistCp(Path src, Path dst) throws Exception {
    DistCpOptions options = new DistCpOptions(Arrays.asList(src), dst);
    Job job = new DistCp(conf, options).execute();
    assertNotNull("Unexpected null job returned from DistCp execution.", job);
    assertTrue("DistCp job did not complete.", job.isComplete());
    assertTrue("DistCp job did not complete successfully.", job.isSuccessful());
  }

  /**
   * Creates a directory and any ancestor directories required.
   *
   * @param fs FileSystem in which to create directories
   * @param dir path of directory to create
   * @throws Exception if there is a failure
   */
  private static void mkdirs(FileSystem fs, Path dir) throws Exception {
    assertTrue("Failed to mkdir " + dir, fs.mkdirs(dir));
  }
}
