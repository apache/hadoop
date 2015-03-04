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
    options.setDeleteMissing(true);
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
    Assert.assertFalse(DistCpSync.sync(options, conf));

    // the source/target does not have the given snapshots
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    Assert.assertFalse(DistCpSync.sync(options, conf));

    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(source, "s2");
    dfs.createSnapshot(target, "s1");
    Assert.assertTrue(DistCpSync.sync(options, conf));

    // changes have been made in target
    final Path subTarget = new Path(target, "sub");
    dfs.mkdirs(subTarget);
    Assert.assertFalse(DistCpSync.sync(options, conf));

    dfs.delete(subTarget, true);
    Assert.assertTrue(DistCpSync.sync(options, conf));
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
  private void changeData(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(foo, "d1");
    final Path f2 = new Path(bar, "f2");

    final Path bar_d1 = new Path(bar, "d1");
    dfs.rename(d1, bar_d1);
    final Path f3 = new Path(bar_d1, "f3");
    dfs.delete(f3, true);
    final Path newfoo = new Path(bar_d1, "foo");
    dfs.rename(foo, newfoo);
    final Path f1 = new Path(newfoo, "f1");
    dfs.delete(f1, true);
    DFSTestUtil.createFile(dfs, f1, 2 * BLOCK_SIZE, DATA_NUM, 0);
    DFSTestUtil.appendFile(dfs, f2, (int) BLOCK_SIZE);
    dfs.rename(bar, new Path(dir, "foo"));
  }

  /**
   * Test the basic functionality.
   */
  @Test
  public void testSync() throws Exception {
    initData(source);
    initData(target);
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(target, "s1");

    // make changes under source
    changeData(source);
    dfs.createSnapshot(source, "s2");

    // do the sync
    Assert.assertTrue(DistCpSync.sync(options, conf));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new GlobbedCopyListing(conf, new Credentials());
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

    // verify that we only copied new appended data of f2 and the new file f1
    Assert.assertEquals(BLOCK_SIZE * 3, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.BYTESCOPIED).getValue());

    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(source), dfs.getFileStatus(target), false);
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
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(target, "s1");

    // make changes under source
    changeData2(source);
    dfs.createSnapshot(source, "s2");

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    // do the sync
    Assert.assertTrue(DistCpSync.sync(options, conf));
    verifyCopy(dfs.getFileStatus(source), dfs.getFileStatus(target), false);
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
   * Test a case where there are multiple source files with the same name
   */
  @Test
  public void testSync3() throws Exception {
    initData3(source);
    initData3(target);
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    dfs.createSnapshot(source, "s1");
    dfs.createSnapshot(target, "s1");

    // make changes under source
    changeData3(source);
    dfs.createSnapshot(source, "s2");

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(source, "s1", "s2");
    System.out.println(report);

    // do the sync
    Assert.assertTrue(DistCpSync.sync(options, conf));
    verifyCopy(dfs.getFileStatus(source), dfs.getFileStatus(target), false);
  }
}
