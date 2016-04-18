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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestDistCpSync {
  private MiniDFSCluster cluster;
  private final Configuration conf = new HdfsConfiguration();
  private DistributedFileSystem dfs;
  private DistCpOptions options;
  private final Path source = new Path("/source");
  private final Path target = new Path("/target");
  private final long BLOCK_SIZE = 1024;
  private final short DATA_NUM = 1;

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATA_NUM).build();
    cluster.waitActive();

    dfs = cluster.getFileSystem();
    dfs.mkdirs(source);
    dfs.mkdirs(target);

    options = new DistCpOptions(Arrays.asList(source), target);
    options.setSyncFolder(true);
    options.setUseDiff(true, "s1", "s2");
    options.appendToConf(conf);

    conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, target.toString());
    conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, target.toString());
  }

  @After
  public void tearDown() throws Exception {
    IOUtils.cleanup(null, dfs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test the sync returns false in the following scenarios:
   * 1. the source/target dir are not snapshottable dir
   * 2. the source/target does not have the given snapshots
   * 3. changes have been made in target
   */
  @Test
  public void testFallback() throws Exception {
    // the source/target dir are not snapshottable dir
    Assert.assertFalse(sync());
    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    // reset source path in options
    options.setSourcePaths(Arrays.asList(source));
    // the source/target does not have the given snapshots
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    Assert.assertFalse(sync());
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    // reset source path in options
    options.setSourcePaths(Arrays.asList(source));
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(source, "s2");
    dfs.createSnapshot(target, "s1");
    Assert.assertTrue(sync());

    // reset source paths in options
    options.setSourcePaths(Arrays.asList(source));
    // changes have been made in target
    final Path subTarget = new Path(target, "sub");
    dfs.mkdirs(subTarget);
    Assert.assertFalse(sync());
    // make sure the source path has been updated to the snapshot path
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    // reset source paths in options
    options.setSourcePaths(Arrays.asList(source));
    dfs.delete(subTarget, true);
    Assert.assertTrue(sync());
  }

  private void enableAndCreateFirstSnapshot() throws Exception {
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(target, "s1");
  }

  private void syncAndVerify() throws Exception {
    Assert.assertTrue(sync());
    verifyCopy(dfs.getFileStatus(source), dfs.getFileStatus(target), false);
  }

  private boolean sync() throws Exception {
    DistCpSync distCpSync = new DistCpSync(options, conf);
    return distCpSync.sync();
  }

  /**
   * create some files and directories under the given directory.
   * the final subtree looks like this:
   *                     dir/
   *              foo/          bar/
   *           d1/    f1     d2/    f2
   *         f3            f4
   */
  private void initData(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(foo, "d1");
    final Path f1 = new Path(foo, "f1");
    final Path d2 = new Path(bar, "d2");
    final Path f2 = new Path(bar, "f2");
    final Path f3 = new Path(d1, "f3");
    final Path f4 = new Path(d2, "f4");

    DFSTestUtil.createFile(dfs, f1, BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.createFile(dfs, f2, BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.createFile(dfs, f3, BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.createFile(dfs, f4, BLOCK_SIZE, DATA_NUM, 0);
  }

  /**
   * make some changes under the given directory (created in the above way).
   * 1. rename dir/foo/d1 to dir/bar/d1
   * 2. delete dir/bar/d1/f3
   * 3. rename dir/foo to /dir/bar/d1/foo
   * 4. delete dir/bar/d1/foo/f1
   * 5. create file dir/bar/d1/foo/f1 whose size is 2*BLOCK_SIZE
   * 6. append one BLOCK to file dir/bar/f2
   * 7. rename dir/bar to dir/foo
   *
   * Thus after all these ops the subtree looks like this:
   *                       dir/
   *                       foo/
   *                 d1/    f2(A)    d2/
   *                foo/             f4
   *                f1(new)
   */
  private int changeData(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(foo, "d1");
    final Path f2 = new Path(bar, "f2");

    final Path bar_d1 = new Path(bar, "d1");
    int numCreatedModified = 0;
    dfs.rename(d1, bar_d1);
    numCreatedModified += 1; // modify ./foo
    numCreatedModified += 1; // modify ./bar
    final Path f3 = new Path(bar_d1, "f3");
    dfs.delete(f3, true);
    final Path newfoo = new Path(bar_d1, "foo");
    dfs.rename(foo, newfoo);
    numCreatedModified += 1; // modify ./foo/d1
    final Path f1 = new Path(newfoo, "f1");
    dfs.delete(f1, true);
    DFSTestUtil.createFile(dfs, f1, 2 * BLOCK_SIZE, DATA_NUM, 0);
    numCreatedModified += 1; // create ./foo/f1
    DFSTestUtil.appendFile(dfs, f2, (int) BLOCK_SIZE);
    numCreatedModified += 1; // modify ./bar/f2
    dfs.rename(bar, new Path(dir, "foo"));
    return numCreatedModified;
  }

  /**
   * Test the basic functionality.
   */
  @Test
  public void testSync() throws Exception {
    initData(source);
    initData(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    int numCreatedModified = changeData(source);
    dfs.createSnapshot(source, "s2");

    // before sync, make some further changes on source. this should not affect
    // the later distcp since we're copying (s2-s1) to target
    final Path toDelete = new Path(source, "foo/d1/foo/f1");
    dfs.delete(toDelete, true);
    final Path newdir = new Path(source, "foo/d1/foo/newdir");
    dfs.mkdirs(newdir);

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    DistCpSync distCpSync = new DistCpSync(options, conf);

    // do the sync
    Assert.assertTrue(distCpSync.sync());

    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
            HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, options);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
        stubContext.getContext();
    // Enable append
    context.getConfiguration().setBoolean(
        DistCpOptionSwitch.APPEND.getConfigLabel(), true);
    copyMapper.setup(context);
    for (Map.Entry<Text, CopyListingFileStatus> entry : copyListing.entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), context);
    }

    // verify that we only list modified and created files/directories
    Assert.assertEquals(numCreatedModified, copyListing.size());

    // verify that we only copied new appended data of f2 and the new file f1
    Assert.assertEquals(BLOCK_SIZE * 3, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.BYTESCOPIED).getValue());

    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  private Map<Text, CopyListingFileStatus> getListing(Path listingPath)
      throws Exception {
    SequenceFile.Reader reader = new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(listingPath));
    Text key = new Text();
    CopyListingFileStatus value = new CopyListingFileStatus();
    Map<Text, CopyListingFileStatus> values = new HashMap<>();
    while (reader.next(key, value)) {
      values.put(key, value);
      key = new Text();
      value = new CopyListingFileStatus();
    }
    return values;
  }

  private void verifyCopy(FileStatus s, FileStatus t, boolean compareName)
      throws Exception {
    Assert.assertEquals(s.isDirectory(), t.isDirectory());
    if (compareName) {
      Assert.assertEquals(s.getPath().getName(), t.getPath().getName());
    }
    if (!s.isDirectory()) {
      // verify the file content is the same
      byte[] sbytes = DFSTestUtil.readFileBuffer(dfs, s.getPath());
      byte[] tbytes = DFSTestUtil.readFileBuffer(dfs, t.getPath());
      Assert.assertArrayEquals(sbytes, tbytes);
    } else {
      FileStatus[] slist = dfs.listStatus(s.getPath());
      FileStatus[] tlist = dfs.listStatus(t.getPath());
      Assert.assertEquals(slist.length, tlist.length);
      for (int i = 0; i < slist.length; i++) {
        verifyCopy(slist[i], tlist[i], true);
      }
    }
  }

  /**
   * Similar test with testSync, but the "to" snapshot is specified as "."
   * @throws Exception
   */
  @Test
  public void testSyncWithCurrent() throws Exception {
    options.setUseDiff(true, "s1", ".");
    initData(source);
    initData(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    changeData(source);

    // do the sync
    sync();
    // make sure the source path is still unchanged
    Assert.assertEquals(source, options.getSourcePaths().get(0));
  }

  private void initData2(Path dir) throws Exception {
    final Path test = new Path(dir, "test");
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path f1 = new Path(test, "f1");
    final Path f2 = new Path(foo, "f2");
    final Path f3 = new Path(bar, "f3");

    DFSTestUtil.createFile(dfs, f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, f2, BLOCK_SIZE, DATA_NUM, 1L);
    DFSTestUtil.createFile(dfs, f3, BLOCK_SIZE, DATA_NUM, 2L);
  }

  private void changeData2(Path dir) throws Exception {
    final Path tmpFoo = new Path(dir, "tmpFoo");
    final Path test = new Path(dir, "test");
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");

    dfs.rename(test, tmpFoo);
    dfs.rename(foo, test);
    dfs.rename(bar, foo);
    dfs.rename(tmpFoo, bar);
  }

  @Test
  public void testSync2() throws Exception {
    initData2(source);
    initData2(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    changeData2(source);
    dfs.createSnapshot(source, "s2");

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    syncAndVerify();
  }

  private void initData3(Path dir) throws Exception {
    final Path test = new Path(dir, "test");
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path f1 = new Path(test, "file");
    final Path f2 = new Path(foo, "file");
    final Path f3 = new Path(bar, "file");

    DFSTestUtil.createFile(dfs, f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, f2, BLOCK_SIZE * 2, DATA_NUM, 1L);
    DFSTestUtil.createFile(dfs, f3, BLOCK_SIZE * 3, DATA_NUM, 2L);
  }

  private void changeData3(Path dir) throws Exception {
    final Path test = new Path(dir, "test");
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path f1 = new Path(test, "file");
    final Path f2 = new Path(foo, "file");
    final Path f3 = new Path(bar, "file");
    final Path newf1 = new Path(test, "newfile");
    final Path newf2 = new Path(foo, "newfile");
    final Path newf3 = new Path(bar, "newfile");

    dfs.rename(f1, newf1);
    dfs.rename(f2, newf2);
    dfs.rename(f3, newf3);
  }

  /**
   * Test a case where there are multiple source files with the same name.
   */
  @Test
  public void testSync3() throws Exception {
    initData3(source);
    initData3(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    changeData3(source);
    dfs.createSnapshot(source, "s2");

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    syncAndVerify();
  }

  private void initData4(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d2 = new Path(d1, "d2");
    final Path f1 = new Path(d2, "f1");

    DFSTestUtil.createFile(dfs, f1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private void changeData4(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d11 = new Path(dir, "d11");
    final Path d2 = new Path(d1, "d2");
    final Path d21 = new Path(d1, "d21");
    final Path f1 = new Path(d2, "f1");

    dfs.delete(f1, false);
    dfs.rename(d2, d21);
    dfs.rename(d1, d11);
  }

  /**
   * Test a case where multiple level dirs are renamed.
   */
  @Test
  public void testSync4() throws Exception {
    initData4(source);
    initData4(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    changeData4(source);
    dfs.createSnapshot(source, "s2");

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    syncAndVerify();
  }

  private void initData5(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d2 = new Path(dir, "d2");
    final Path f1 = new Path(d1, "f1");
    final Path f2 = new Path(d2, "f2");

    DFSTestUtil.createFile(dfs, f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, f2, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private void changeData5(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d2 = new Path(dir, "d2");
    final Path f1 = new Path(d1, "f1");
    final Path tmp = new Path(dir, "tmp");

    dfs.delete(f1, false);
    dfs.rename(d1, tmp);
    dfs.rename(d2, d1);
    final Path f2 = new Path(d1, "f2");
    dfs.delete(f2, false);
  }

   /**
   * Test a case with different delete and rename sequences.
   */
  @Test
  public void testSync5() throws Exception {
    initData5(source);
    initData5(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    changeData5(source);
    dfs.createSnapshot(source, "s2");

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    syncAndVerify();
  }

  private void testAndVerify(int numCreatedModified)
          throws Exception{
    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    DistCpSync distCpSync = new DistCpSync(options, conf);
    // do the sync
    Assert.assertTrue(distCpSync.sync());

    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
            HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, options.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, options);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
            stubContext.getContext();
    // Enable append
    context.getConfiguration().setBoolean(
            DistCpOptionSwitch.APPEND.getConfigLabel(), true);
    copyMapper.setup(context);
    for (Map.Entry<Text, CopyListingFileStatus> entry :
            copyListing.entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), context);
    }

    // verify that we only list modified and created files/directories
    Assert.assertEquals(numCreatedModified, copyListing.size());

    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  private void initData6(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path foo_f1 = new Path(foo, "f1");
    final Path bar_f1 = new Path(bar, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, bar_f1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private int changeData6(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path foo2 = new Path(dir, "foo2");
    final Path foo_f1 = new Path(foo, "f1");

    int numCreatedModified = 0;
    dfs.rename(foo, foo2);
    dfs.rename(bar, foo);
    dfs.rename(foo2, bar);
    DFSTestUtil.appendFile(dfs, foo_f1, (int) BLOCK_SIZE);
    numCreatedModified += 1; // modify ./bar/f1
    return numCreatedModified;
  }

  /**
   * Test a case where there is a cycle in renaming dirs.
   */
  @Test
  public void testSync6() throws Exception {
    initData6(source);
    initData6(target);
    enableAndCreateFirstSnapshot();
    int numCreatedModified = changeData6(source);
    dfs.createSnapshot(source, "s2");

    testAndVerify(numCreatedModified);
  }

  private void initData7(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path foo_f1 = new Path(foo, "f1");
    final Path bar_f1 = new Path(bar, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, bar_f1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private int changeData7(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path foo2 = new Path(dir, "foo2");
    final Path foo_f1 = new Path(foo, "f1");
    final Path foo2_f2 = new Path(foo2, "f2");
    final Path foo_d1 = new Path(foo, "d1");
    final Path foo_d1_f3 = new Path(foo_d1, "f3");

    int numCreatedModified = 0;
    dfs.rename(foo, foo2);
    DFSTestUtil.createFile(dfs, foo_f1, BLOCK_SIZE, DATA_NUM, 0L);
    numCreatedModified += 2; // create ./foo and ./foo/f1
    DFSTestUtil.appendFile(dfs, foo_f1, (int) BLOCK_SIZE);
    dfs.rename(foo_f1, foo2_f2);
    numCreatedModified -= 1; // mv ./foo/f1
    numCreatedModified += 2; // "M ./foo" and "+ ./foo/f2"
    DFSTestUtil.createFile(dfs, foo_d1_f3, BLOCK_SIZE, DATA_NUM, 0L);
    numCreatedModified += 2; // create ./foo/d1 and ./foo/d1/f3
    return numCreatedModified;
  }

  /**
   * Test a case where rename a dir, then create a new dir with the same name
   * and sub dir.
   */
  @Test
  public void testSync7() throws Exception {
    initData7(source);
    initData7(target);
    enableAndCreateFirstSnapshot();
    int numCreatedModified = changeData7(source);
    dfs.createSnapshot(source, "s2");

    testAndVerify(numCreatedModified);
  }

  private void initData8(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(dir, "d1");
    final Path foo_f1 = new Path(foo, "f1");
    final Path bar_f1 = new Path(bar, "f1");
    final Path d1_f1 = new Path(d1, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, bar_f1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, d1_f1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private int changeData8(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path createdDir = new Path(dir, "c");
    final Path d1 = new Path(dir, "d1");
    final Path d1_f1 = new Path(d1, "f1");
    final Path createdDir_f1 = new Path(createdDir, "f1");
    final Path foo_f3 = new Path(foo, "f3");
    final Path new_foo = new Path(createdDir, "foo");
    final Path foo_f4 = new Path(foo, "f4");
    final Path foo_d1 = new Path(foo, "d1");
    final Path bar = new Path(dir, "bar");
    final Path bar1 = new Path(dir, "bar1");

    int numCreatedModified = 0;
    DFSTestUtil.createFile(dfs, foo_f3, BLOCK_SIZE, DATA_NUM, 0L);
    numCreatedModified += 1; // create  ./c/foo/f3
    DFSTestUtil.createFile(dfs, createdDir_f1, BLOCK_SIZE, DATA_NUM, 0L);
    numCreatedModified += 1; // create ./c
    dfs.rename(createdDir_f1, foo_f4);
    numCreatedModified += 1; // create ./c/foo/f4
    dfs.rename(d1_f1, createdDir_f1); // rename ./d1/f1 -> ./c/f1
    numCreatedModified += 1; // modify ./c/foo/d1
    dfs.rename(d1, foo_d1);
    numCreatedModified += 1; // modify ./c/foo
    dfs.rename(foo, new_foo);
    dfs.rename(bar, bar1);
    return numCreatedModified;
  }

  /**
   * Test a case where create a dir, then mv a existed dir into it.
   */
  @Test
  public void testSync8() throws Exception {
    initData8(source);
    initData8(target);
    enableAndCreateFirstSnapshot();
    int numCreatedModified = changeData8(source);
    dfs.createSnapshot(source, "s2");

    testAndVerify(numCreatedModified);
  }

  private void initData9(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path foo_f1 = new Path(foo, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private void changeData9(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path foo_f2 = new Path(foo, "f2");

    DFSTestUtil.createFile(dfs, foo_f2, BLOCK_SIZE, DATA_NUM, 0L);
  }

  /**
   * Test a case where the source path is relative.
   */
  @Test
  public void testSync9() throws Exception {

    // use /user/$USER/source for source directory
    Path sourcePath = new Path(dfs.getWorkingDirectory(), "source");
    initData9(sourcePath);
    initData9(target);
    dfs.allowSnapshot(sourcePath);
    dfs.allowSnapshot(target);
    dfs.createSnapshot(sourcePath, "s1");
    dfs.createSnapshot(target, "s1");
    changeData9(sourcePath);
    dfs.createSnapshot(sourcePath, "s2");

    String[] args = new String[]{"-update","-diff", "s1", "s2",
                                   "source", target.toString()};
    new DistCp(conf, OptionsParser.parse(args)).execute();
    verifyCopy(dfs.getFileStatus(sourcePath),
                 dfs.getFileStatus(target), false);
  }
}
