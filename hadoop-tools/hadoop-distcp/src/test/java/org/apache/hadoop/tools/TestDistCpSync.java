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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestDistCpSync {
  private MiniDFSCluster cluster;
  private final Configuration conf = new HdfsConfiguration();
  private DistributedFileSystem dfs;
  private WebHdfsFileSystem webfs;
  private DistCpContext context;
  private final Path source = new Path("/source");
  private final Path target = new Path("/target");
  private final long BLOCK_SIZE = 1024;
  private final short DATA_NUM = 1;

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATA_NUM).build();
    cluster.waitActive();

    webfs = WebHdfsTestUtil.
            getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);

    dfs = cluster.getFileSystem();
    dfs.mkdirs(source);
    dfs.mkdirs(target);

    final DistCpOptions options = new DistCpOptions.Builder(
        Collections.singletonList(source), target)
        .withSyncFolder(true)
        .withUseDiff("s1", "s2")
        .build();
    options.appendToConf(conf);
    context = new DistCpContext(options);

    conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, target.toString());
    conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, target.toString());
    conf.setClass("fs.dummy.impl", DummyFs.class, FileSystem.class);
  }

  @After
  public void tearDown() throws Exception {
    IOUtils.cleanupWithLogger(null, dfs);
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
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // reset source path in options
    context.setSourcePaths(Collections.singletonList(source));
    // the source/target does not have the given snapshots
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    Assert.assertFalse(sync());
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // reset source path in options
    context.setSourcePaths(Collections.singletonList(source));
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(source, "s2");
    dfs.createSnapshot(target, "s1");
    Assert.assertTrue(sync());

    // reset source paths in options
    context.setSourcePaths(Collections.singletonList(source));
    // changes have been made in target
    final Path subTarget = new Path(target, "sub");
    dfs.mkdirs(subTarget);
    Assert.assertFalse(sync());
    // make sure the source path has been updated to the snapshot path
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // reset source paths in options
    context.setSourcePaths(Collections.singletonList(source));
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
    DistCpSync distCpSync = new DistCpSync(context, conf);
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
    initData(dfs, dir);
  }

  private void initData(FileSystem fs, Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(foo, "d1");
    final Path f1 = new Path(foo, "f1");
    final Path d2 = new Path(bar, "d2");
    final Path f2 = new Path(bar, "f2");
    final Path f3 = new Path(d1, "f3");
    final Path f4 = new Path(d2, "f4");

    DFSTestUtil.createFile(fs, f1, BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.createFile(fs, f2, BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.createFile(fs, f3, BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.createFile(fs, f4, BLOCK_SIZE, DATA_NUM, 0);
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
  private int changeData(FileSystem fs, Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(foo, "d1");
    final Path f2 = new Path(bar, "f2");

    final Path bar_d1 = new Path(bar, "d1");
    int numCreatedModified = 0;
    fs.rename(d1, bar_d1);
    numCreatedModified += 1; // modify ./foo
    numCreatedModified += 1; // modify ./bar
    final Path f3 = new Path(bar_d1, "f3");
    fs.delete(f3, true);
    final Path newfoo = new Path(bar_d1, "foo");
    fs.rename(foo, newfoo);
    numCreatedModified += 1; // modify ./foo/d1
    final Path f1 = new Path(newfoo, "f1");
    fs.delete(f1, true);
    DFSTestUtil.createFile(fs, f1, 2 * BLOCK_SIZE, DATA_NUM, 0);
    numCreatedModified += 1; // create ./foo/f1
    DFSTestUtil.appendFile(fs, f2, (int) BLOCK_SIZE);
    numCreatedModified += 1; // modify ./bar/f2
    fs.rename(bar, new Path(dir, "foo"));
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
    int numCreatedModified = changeData(dfs, source);
    dfs.createSnapshot(source, "s2");

    // before sync, make some further changes on source. this should not affect
    // the later distcp since we're copying (s2-s1) to target
    final Path toDelete = new Path(source, "foo/d1/foo/f1");
    dfs.delete(toDelete, true);
    final Path newdir = new Path(source, "foo/d1/foo/newdir");
    dfs.mkdirs(newdir);

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    DistCpSync distCpSync = new DistCpSync(context, conf);

    // do the sync
    Assert.assertTrue(distCpSync.sync());

    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
            HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, context);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapContext =
        stubContext.getContext();
    // Enable append
    mapContext.getConfiguration().setBoolean(
        DistCpOptionSwitch.APPEND.getConfigLabel(), true);
    copyMapper.setup(mapContext);
    for (Map.Entry<Text, CopyListingFileStatus> entry : copyListing.entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), mapContext);
    }

    // verify that we only list modified and created files/directories
    Assert.assertEquals(numCreatedModified, copyListing.size());

    // verify that we only copied new appended data of f2 and the new file f1
    Assert.assertEquals(BLOCK_SIZE * 3, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.BYTESCOPIED).getValue());

    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  /**
   * Test the basic functionality.
   */
  @Test
  public void testSync1() throws Exception {
    Path srcpath = new Path(source, "encz-mock");
    dfs.mkdirs(srcpath);
    dfs.mkdirs(new Path(source, "encz-mock/datedir"));
    enableAndCreateFirstSnapshot();

    // before sync, make some further changes on source
    DFSTestUtil.createFile(dfs, new Path(source, "encz-mock/datedir/file1"),
        BLOCK_SIZE, DATA_NUM, 0);
    dfs.delete(new Path(source, "encz-mock/datedir"), true);
    dfs.mkdirs(new Path(source, "encz-mock/datedir"));
    DFSTestUtil.createFile(dfs, new Path(source, "encz-mock/datedir/file2"),
        BLOCK_SIZE, DATA_NUM, 0);
    dfs.createSnapshot(source, "s2");
    Assert.assertTrue(dfs.exists(new Path(source, "encz-mock/datedir/file2")));

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    DistCpSync distCpSync = new DistCpSync(context, conf);

    // do the sync
    Assert.assertTrue(distCpSync.sync());
    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing =
        new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, context);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapContext =
        stubContext.getContext();
    copyMapper.setup(mapContext);
    for (Map.Entry<Text, CopyListingFileStatus> entry : copyListing
        .entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), mapContext);
    }
    Assert.assertTrue(dfs.exists(new Path(target, "encz-mock/datedir/file2")));
    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  /**
   * Test the basic functionality.
   */
  @Test
  public void testSyncNew() throws Exception {
    Path srcpath = new Path(source, "encz-mock");
    dfs.mkdirs(srcpath);
    dfs.mkdirs(new Path(source, "encz-mock/datedir"));
    dfs.mkdirs(new Path(source, "trash"));
    enableAndCreateFirstSnapshot();

    // before sync, make some further changes on source
    DFSTestUtil.createFile(dfs, new Path(source, "encz-mock/datedir/file1"),
        BLOCK_SIZE, DATA_NUM, 0);
    dfs.rename(new Path(source, "encz-mock/datedir"),
        new Path(source, "trash"));
    dfs.mkdirs(new Path(source, "encz-mock/datedir"));
    DFSTestUtil.createFile(dfs, new Path(source, "encz-mock/datedir/file2"),
        BLOCK_SIZE, DATA_NUM, 0);
    dfs.createSnapshot(source, "s2");
    Assert.assertTrue(dfs.exists(new Path(source, "encz-mock/datedir/file2")));

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    DistCpSync distCpSync = new DistCpSync(context, conf);

    // do the sync
    Assert.assertTrue(distCpSync.sync());
    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing =
        new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, context);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapContext =
        stubContext.getContext();
    copyMapper.setup(mapContext);
    for (Map.Entry<Text, CopyListingFileStatus> entry : copyListing
        .entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), mapContext);
    }
    Assert.assertTrue(dfs.exists(new Path(target, "encz-mock/datedir/file2")));
    Assert.assertTrue(dfs.exists(new Path(target, "trash/datedir/file1")));
    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  /**
   * Test the basic functionality.
   */
  @Test
  public void testSyncWithFilters() throws Exception {
    Path srcpath = new Path(source, "encz-mock");
    dfs.mkdirs(srcpath);
    dfs.mkdirs(new Path(source, "encz-mock/datedir"));
    dfs.mkdirs(new Path(source, "trash"));
    enableAndCreateFirstSnapshot();

    // before sync, make some further changes on source
    DFSTestUtil.createFile(dfs, new Path(source, "encz-mock/datedir/file1"),
        BLOCK_SIZE, DATA_NUM, 0);
    dfs.rename(new Path(source, "encz-mock/datedir"),
        new Path(source, "trash"));
    dfs.mkdirs(new Path(source, "encz-mock/datedir"));
    DFSTestUtil.createFile(dfs, new Path(source, "encz-mock/datedir/file2"),
        BLOCK_SIZE, DATA_NUM, 0);
    dfs.createSnapshot(source, "s2");
    Assert.assertTrue(dfs.exists(new Path(source, "encz-mock/datedir/file2")));

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);
    List<Pattern> filters = new ArrayList<>();
    filters.add(Pattern.compile(".*trash.*"));
    RegexCopyFilter regexCopyFilter = new RegexCopyFilter("fakeFile");
    regexCopyFilter.setFilters(filters);

    DistCpSync distCpSync = new DistCpSync(context, conf);
    distCpSync.setCopyFilter(regexCopyFilter);

    // do the sync
    Assert.assertTrue(distCpSync.sync());
    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing =
        new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, context);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapContext =
        stubContext.getContext();
    copyMapper.setup(mapContext);
    for (Map.Entry<Text, CopyListingFileStatus> entry : copyListing
        .entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), mapContext);
    }
    Assert.assertTrue(dfs.exists(new Path(target, "encz-mock/datedir/file2")));
    Assert.assertFalse(dfs.exists(new Path(target, "encz-mock/datedir/file1")));
    Assert.assertFalse(dfs.exists(new Path(target, "trash/datedir/file1")));
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

  /**
   * By default, we are using DFS for both source and target.
   * @param s source file status
   * @param t target file status
   * @param compareName whether will we compare the name of the files
   * @throws Exception
   */
  private void verifyCopy(FileStatus s, FileStatus t, boolean compareName)
          throws Exception {
    verifyCopy(dfs, dfs, s, t, compareName);
  }

  /**
   * Verify copy by using different file systems.
   * @param sfs source file system
   * @param tfs target file system
   * @param s source file status
   * @param t target file status
   * @param compareName whether will we compare the name of the files
   * @throws Exception
   */
  private void verifyCopyByFs(FileSystem sfs, FileSystem tfs,
                              FileStatus s, FileStatus t, boolean compareName)
          throws Exception {
    verifyCopy(sfs, tfs, s, t, compareName);
  }

  private void verifyCopy(FileSystem sfs, FileSystem tfs,
                          FileStatus s, FileStatus t, boolean compareName)
          throws Exception {
    Assert.assertEquals(s.isDirectory(), t.isDirectory());
    if (compareName) {
      Assert.assertEquals(s.getPath().getName(), t.getPath().getName());
    }
    if (!s.isDirectory()) {
      // verify the file content is the same
      byte[] sbytes = DFSTestUtil.readFileBuffer(sfs, s.getPath());
      byte[] tbytes = DFSTestUtil.readFileBuffer(tfs, t.getPath());
      Assert.assertArrayEquals(sbytes, tbytes);
    } else {
      FileStatus[] slist = sfs.listStatus(s.getPath());
      FileStatus[] tlist = tfs.listStatus(t.getPath());
      Assert.assertEquals(slist.length, tlist.length);
      for (int i = 0; i < slist.length; i++) {
        verifyCopy(sfs, tfs, slist[i], tlist[i], true);
      }
    }
  }

  /**
   * Similar test with testSync, but the "to" snapshot is specified as "."
   * @throws Exception
   */
  @Test
  public void testSyncWithCurrent() throws Exception {
    final DistCpOptions options = new DistCpOptions.Builder(
        Collections.singletonList(source), target)
        .withSyncFolder(true)
        .withUseDiff("s1", ".")
        .build();
    context = new DistCpContext(options);
    initData(source);
    initData(target);
    enableAndCreateFirstSnapshot();

    // make changes under source
    changeData(dfs, source);

    // do the sync
    sync();
    // make sure the source path is still unchanged
    Assert.assertEquals(source, context.getSourcePaths().get(0));
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

    DistCpSync distCpSync = new DistCpSync(context, conf);
    // do the sync
    Assert.assertTrue(distCpSync.sync());

    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
            HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s2");
    Assert.assertEquals(spath, context.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, context);

    Map<Text, CopyListingFileStatus> copyListing = getListing(listingPath);
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapContext =
            stubContext.getContext();
    // Enable append
    mapContext.getConfiguration().setBoolean(
            DistCpOptionSwitch.APPEND.getConfigLabel(), true);
    copyMapper.setup(mapContext);
    for (Map.Entry<Text, CopyListingFileStatus> entry :
            copyListing.entrySet()) {
      copyMapper.map(entry.getKey(), entry.getValue(), mapContext);
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

  @Test
  public void testSyncSnapshotTimeStampChecking() throws Exception {
    initData(source);
    initData(target);
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    dfs.createSnapshot(source, "s2");
    dfs.createSnapshot(target, "s1");
    // Sleep one second to make snapshot s1 created later than s2
    Thread.sleep(1000);
    dfs.createSnapshot(source, "s1");

    boolean threwException = false;
    try {
      DistCpSync distCpSync = new DistCpSync(context, conf);
      // do the sync
      distCpSync.sync();
    } catch (HadoopIllegalArgumentException e) {
      threwException = true;
      GenericTestUtils.assertExceptionContains(
          "Snapshot s2 should be newer than s1", e);
    }
    Assert.assertTrue(threwException);
  }

  private void initData10(Path dir) throws Exception {
    final Path staging = new Path(dir, ".staging");
    final Path stagingF1 = new Path(staging, "f1");
    final Path data = new Path(dir, "data");
    final Path dataF1 = new Path(data, "f1");

    DFSTestUtil.createFile(dfs, stagingF1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, dataF1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private void changeData10(Path dir) throws Exception {
    final Path staging = new Path(dir, ".staging");
    final Path prod = new Path(dir, "prod");
    dfs.rename(staging, prod);
  }

  private java.nio.file.Path generateFilterFile(String fileName)
          throws IOException {
    java.nio.file.Path tmpFile = Files.createTempFile(fileName, "txt");
    String str = ".*\\.staging.*";
    try (BufferedWriter writer = new BufferedWriter(
            new FileWriter(tmpFile.toString()))) {
      writer.write(str);
    }
    return tmpFile;
  }

  private void deleteFilterFile(java.nio.file.Path filePath)
          throws IOException {
    Files.delete(filePath);
  }

  @Test
  public void testSync10() throws Exception {
    java.nio.file.Path filterFile = null;
    try {
      Path sourcePath = new Path(dfs.getWorkingDirectory(), "source");
      initData10(sourcePath);
      dfs.allowSnapshot(sourcePath);
      dfs.createSnapshot(sourcePath, "s1");
      filterFile = generateFilterFile("filters");
      final DistCpOptions.Builder builder = new DistCpOptions.Builder(
              new ArrayList<>(Arrays.asList(sourcePath)),
              target)
              .withFiltersFile(filterFile.toString())
              .withSyncFolder(true);
      new DistCp(conf, builder.build()).execute();
      verifySync(dfs.getFileStatus(sourcePath),
              dfs.getFileStatus(target), false, ".staging");

      dfs.allowSnapshot(target);
      dfs.createSnapshot(target, "s1");
      changeData10(sourcePath);
      dfs.createSnapshot(sourcePath, "s2");

      final DistCpOptions.Builder diffBuilder = new DistCpOptions.Builder(
              new ArrayList<>(Arrays.asList(sourcePath)),
              target)
              .withUseDiff("s1", "s2")
              .withFiltersFile(filterFile.toString())
              .withSyncFolder(true);
      new DistCp(conf, diffBuilder.build()).execute();
      verifyCopy(dfs.getFileStatus(sourcePath),
              dfs.getFileStatus(target), false);
    } finally {
      deleteFilterFile(filterFile);
    }
  }

  private void initData11(Path dir) throws Exception {
    final Path staging = new Path(dir, "prod");
    final Path stagingF1 = new Path(staging, "f1");
    final Path data = new Path(dir, "data");
    final Path dataF1 = new Path(data, "f1");

    DFSTestUtil.createFile(dfs, stagingF1, BLOCK_SIZE, DATA_NUM, 0L);
    DFSTestUtil.createFile(dfs, dataF1, BLOCK_SIZE, DATA_NUM, 0L);
  }

  private void changeData11(Path dir) throws Exception {
    final Path staging = new Path(dir, "prod");
    final Path prod = new Path(dir, ".staging");
    dfs.rename(staging, prod);
  }

  private void verifySync(FileStatus s, FileStatus t, boolean compareName,
                          String deletedName)
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
      int minFiles = tlist.length;
      if (slist.length < tlist.length) {
        minFiles = slist.length;
      }
      for (int i = 0; i < minFiles; i++) {
        if (slist[i].getPath().getName().contains(deletedName)) {
          if (tlist[i].getPath().getName().contains(deletedName)) {
            throw new Exception("Target is not synced as per exclusion filter");
          }
          continue;
        }
        verifySync(slist[i], tlist[i], true, deletedName);
      }
    }
  }

  @Test
  public void testSync11() throws Exception {
    java.nio.file.Path filterFile = null;
    try {
      Path sourcePath = new Path(dfs.getWorkingDirectory(), "source");
      initData11(sourcePath);
      dfs.allowSnapshot(sourcePath);
      dfs.createSnapshot(sourcePath, "s1");
      filterFile = generateFilterFile("filters");
      final DistCpOptions.Builder builder = new DistCpOptions.Builder(
              new ArrayList<>(Arrays.asList(sourcePath)),
              target)
              .withFiltersFile(filterFile.toString())
              .withSyncFolder(true);
      new DistCp(conf, builder.build()).execute();
      verifyCopy(dfs.getFileStatus(sourcePath),
              dfs.getFileStatus(target), false);

      dfs.allowSnapshot(target);
      dfs.createSnapshot(target, "s1");
      changeData11(sourcePath);
      dfs.createSnapshot(sourcePath, "s2");

      final DistCpOptions.Builder diffBuilder = new DistCpOptions.Builder(
              new ArrayList<>(Arrays.asList(sourcePath)),
              target)
              .withUseDiff("s1", "s2")
              .withFiltersFile(filterFile.toString())
              .withSyncFolder(true);
      new DistCp(conf, diffBuilder.build()).execute();
      verifySync(dfs.getFileStatus(sourcePath),
              dfs.getFileStatus(target), false, ".staging");
    } finally {
      deleteFilterFile(filterFile);
    }
  }

  /**
   * Test DistCp ues diff option under (s)WebHDFSFileSyste.
   * In this test, we are using DFS as source and WebHDFS as target
   */
  @Test
  public void testSyncSnapshotDiffWithWebHdfs1() throws Exception {
    Path dfsSource = new Path(dfs.getUri().toString(), source);
    Path webHdfsTarget = new Path(webfs.getUri().toString(), target);

    snapshotDiffWithPaths(dfsSource, webHdfsTarget);
  }

  /**
   * Test DistCp ues diff option under (s)WebHDFSFileSyste.
   * In this test, we are using WebHDFS as source and DFS as target
   */
  @Test
  public void testSyncSnapshotDiffWithWebHdfs2() throws Exception {
    Path webHdfsSource = new Path(webfs.getUri().toString(), source);
    Path dfsTarget = new Path(dfs.getUri().toString(), target);

    snapshotDiffWithPaths(webHdfsSource, dfsTarget);
  }

  /**
   * Test DistCp ues diff option under (s)WebHDFSFileSyste.
   * In this test, we are using WebHDFS for both source and target
   */
  @Test
  public void testSyncSnapshotDiffWithWebHdfs3() throws Exception {
    Path webHdfsSource = new Path(webfs.getUri().toString(), source);
    Path webHdfsTarget = new Path(webfs.getUri().toString(), target);

    snapshotDiffWithPaths(webHdfsSource, webHdfsTarget);
  }

  private void snapshotDiffWithPaths(Path sourceFSPath,
      Path targetFSPath) throws Exception {

    FileSystem sourceFS = sourceFSPath.getFileSystem(conf);
    FileSystem targetFS = targetFSPath.getFileSystem(conf);

    // Initialize both source and target file system
    initData(sourceFS, sourceFSPath);
    initData(targetFS, targetFSPath);

    // create snapshots on both source and target side with the same name
    List<Path> paths = Arrays.asList(sourceFSPath, targetFSPath);
    for (Path path: paths) {
      FileSystem fs = path.getFileSystem(conf);
      if (fs instanceof DistributedFileSystem) {
        ((DistributedFileSystem)fs).allowSnapshot(path);
      } else if (fs instanceof WebHdfsFileSystem) {
        ((WebHdfsFileSystem)fs).allowSnapshot(path);
      } else {
        throw new IOException("Unsupported fs: " + fs.getScheme());
      }
      fs.createSnapshot(path, "s1");
    }

    // do some modification on source side
    changeData(sourceFS, sourceFSPath);

    // create a new snapshot on source side
    sourceFS.createSnapshot(sourceFSPath, "s2");

    //try to copy the difference
    final DistCpOptions options = new DistCpOptions.Builder(
        Collections.singletonList(sourceFSPath), targetFSPath)
        .withUseDiff("s1", "s2")
        .withSyncFolder(true)
        .build();
    options.appendToConf(conf);

    new DistCp(conf, options).execute();

    verifyCopyByFs(sourceFS, targetFS, sourceFS.getFileStatus(sourceFSPath),
        targetFS.getFileStatus(targetFSPath), false);
  }

  @Test
  public void testRenameWithFilter() throws Exception {
    java.nio.file.Path filterFile = null;
    try {
      Path sourcePath = new Path(dfs.getWorkingDirectory(), "source");

      // Create some dir inside source
      dfs.mkdirs(new Path(sourcePath, "dir1"));
      dfs.mkdirs(new Path(sourcePath, "dir2"));

      // Allow & Create snapshot at source.
      dfs.allowSnapshot(sourcePath);
      dfs.createSnapshot(sourcePath, "s1");

      filterFile = Files.createTempFile("filters", "txt");
      String str = ".*filterDir1.*";
      try (BufferedWriter writer = new BufferedWriter(
          new FileWriter(filterFile.toString()))) {
        writer.write(str);
      }
      final DistCpOptions.Builder builder =
          new DistCpOptions.Builder(new ArrayList<>(Arrays.asList(sourcePath)),
              target).withFiltersFile(filterFile.toString())
              .withSyncFolder(true);
      new DistCp(conf, builder.build()).execute();

      // Check the two directories get copied.
      ContractTestUtils
          .assertPathExists(dfs, "dir1 should get copied to target",
              new Path(target, "dir1"));
      ContractTestUtils
          .assertPathExists(dfs, "dir2 should get copied to target",
              new Path(target, "dir2"));

      // Allow & create initial snapshots on target.
      dfs.allowSnapshot(target);
      dfs.createSnapshot(target, "s1");

      // Now do a rename to a filtered name on source.
      dfs.rename(new Path(sourcePath, "dir1"),
          new Path(sourcePath, "filterDir1"));

      ContractTestUtils
          .assertPathExists(dfs, "'filterDir1' should be there on source",
              new Path(sourcePath, "filterDir1"));

      // Create the incremental snapshot.
      dfs.createSnapshot(sourcePath, "s2");

      final DistCpOptions.Builder diffBuilder =
          new DistCpOptions.Builder(new ArrayList<>(Arrays.asList(sourcePath)),
              target).withUseDiff("s1", "s2")
              .withFiltersFile(filterFile.toString()).withSyncFolder(true);
      new DistCp(conf, diffBuilder.build()).execute();

      // Check the only qualified directory dir2 is there in target
      ContractTestUtils.assertPathExists(dfs, "dir2 should be there on target",
          new Path(target, "dir2"));

      // Check the filtered directory is not there.
      ContractTestUtils.assertPathDoesNotExist(dfs,
          "Filtered directory 'filterDir1' shouldn't get copied",
          new Path(target, "filterDir1"));

      // Check the renamed directory gets deleted.
      ContractTestUtils.assertPathDoesNotExist(dfs,
          "Renamed directory 'dir1' should get deleted",
          new Path(target, "dir1"));

      // Check the filtered directory isn't there in the home directory.
      ContractTestUtils.assertPathDoesNotExist(dfs,
          "Filtered directory 'filterDir1' shouldn't get copied to home directory",
          new Path("filterDir1"));
    } finally {
      deleteFilterFile(filterFile);
    }
  }

  @Test
  public void testSyncSnapshotDiffWithLocalFileSystem() throws Exception {
    String[] args = new String[]{"-update", "-diff", "s1", "s2",
        "file:///source", "file:///target"};
    LambdaTestUtils.intercept(
        UnsupportedOperationException.class,
        "The source file system file does not support snapshot",
        () -> new DistCp(conf, OptionsParser.parse(args)).execute());
  }

  @Test
  public void testSyncSnapshotDiffWithDummyFileSystem() {
    String[] args =
        new String[] { "-update", "-diff", "s1", "s2", "dummy:///source",
            "dummy:///target" };
    try {
      FileSystem dummyFs = FileSystem.get(URI.create("dummy:///"), conf);
      assertThat(dummyFs).isInstanceOf(DummyFs.class);
      new DistCp(conf, OptionsParser.parse(args)).execute();
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      // can expect other exceptions as source and target paths
      // are not created.
    }
  }

  public static class DummyFs extends RawLocalFileSystem {
    public DummyFs() {
      super();
    }

    public URI getUri() {
      return URI.create("dummy:///");
    }

    @Override
    public boolean hasPathCapability(Path path, String capability)
        throws IOException {
      switch (validatePathCapabilityArgs(makeQualified(path), capability)) {
      case CommonPathCapabilities.FS_SNAPSHOTS:
        return true;
      default:
        return super.hasPathCapability(path, capability);
      }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus();
    }

    public SnapshotDiffReport getSnapshotDiffReport(final Path snapshotDir,
        final String fromSnapshot, final String toSnapshot) {
      return new SnapshotDiffReport(snapshotDir.getName(), fromSnapshot,
          toSnapshot, new ArrayList<SnapshotDiffReport.DiffReportEntry>());
    }
  }
}
