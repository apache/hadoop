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
import org.apache.hadoop.fs.FsShell;
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Base class to test "-rdiff s2 s1".
 * Shared by "-rdiff s2 s1 src tgt" and "-rdiff s2 s1 tgt tgt"
 */
public abstract class TestDistCpSyncReverseBase {
  private MiniDFSCluster cluster;
  private final Configuration conf = new HdfsConfiguration();
  private DistributedFileSystem dfs;
  private DistCpOptions.Builder optionsBuilder;
  private DistCpContext distCpContext;
  private Path source;
  private boolean isSrcNotSameAsTgt = true;
  private final Path target = new Path("/target");
  private final long blockSize = 1024;
  private final short dataNum = 1;

  abstract void initSourcePath();

  private static List<String> lsr(final String prefix,
      final FsShell shell, Path rootDir) throws Exception {
    return lsr(prefix, shell, rootDir.toString(), null);
  }

  private List<String> lsrSource(final String prefix,
      final FsShell shell, Path rootDir) throws Exception {
    final Path spath = isSrcNotSameAsTgt? rootDir :
      new Path(rootDir.toString(),
          HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s1");
    return lsr(prefix, shell, spath.toString(), null);
  }

  private static List<String> lsr(final String prefix,
      final FsShell shell, String rootDir, String glob) throws Exception {
    final String dir = glob == null ? rootDir : glob;
    System.out.println(prefix + " lsr root=" + rootDir);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      Assert.assertEquals(0, shell.run(new String[] {"-lsr", dir }));
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("lsr results:\n" + results);
    String dirname = rootDir;
    if (rootDir.lastIndexOf(Path.SEPARATOR) != -1) {
      dirname = rootDir.substring(rootDir.lastIndexOf(Path.SEPARATOR));
    }

    final List<String> paths = new ArrayList<String>();
    for (StringTokenizer t = new StringTokenizer(results, "\n"); t
        .hasMoreTokens();) {
      final String s = t.nextToken();
      final int i = s.indexOf(dirname);
      if (i >= 0) {
        paths.add(s.substring(i + dirname.length()));
      }
    }
    Collections.sort(paths);
    System.out
        .println("lsr paths = " + paths.toString().replace(", ", ",\n  "));
    return paths;
  }

  public void setSource(final Path src) {
    this.source = src;
  }

  public void setSrcNotSameAsTgt(final boolean srcNotSameAsTgt) {
    isSrcNotSameAsTgt = srcNotSameAsTgt;
  }

  @Before
  public void setUp() throws Exception {
    initSourcePath();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(dataNum).build();
    cluster.waitActive();

    dfs = cluster.getFileSystem();
    if (isSrcNotSameAsTgt) {
      dfs.mkdirs(source);
    }
    dfs.mkdirs(target);

    optionsBuilder = new DistCpOptions.Builder(Arrays.asList(source), target)
        .withSyncFolder(true)
        .withUseRdiff("s2", "s1");
    final DistCpOptions options = optionsBuilder.build();
    options.appendToConf(conf);
    distCpContext = new DistCpContext(options);

    conf.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH, target.toString());
    conf.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH, target.toString());
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
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s1");
    Assert.assertEquals(spath, distCpContext.getSourcePaths().get(0));

    // reset source path in options
    optionsBuilder.withSourcePaths(Arrays.asList(source));
    // the source/target does not have the given snapshots
    dfs.allowSnapshot(source);
    dfs.allowSnapshot(target);
    Assert.assertFalse(sync());
    Assert.assertEquals(spath, distCpContext.getSourcePaths().get(0));

    // reset source path in options
    optionsBuilder.withSourcePaths(Arrays.asList(source));
    this.enableAndCreateFirstSnapshot();
    dfs.createSnapshot(target, "s2");
    Assert.assertTrue(sync());

    // reset source paths in options
    optionsBuilder.withSourcePaths(Arrays.asList(source));
    // changes have been made in target
    final Path subTarget = new Path(target, "sub");
    dfs.mkdirs(subTarget);
    Assert.assertFalse(sync());
    // make sure the source path has been updated to the snapshot path
    Assert.assertEquals(spath, distCpContext.getSourcePaths().get(0));

    // reset source paths in options
    optionsBuilder.withSourcePaths(Arrays.asList(source));
    dfs.delete(subTarget, true);
    Assert.assertTrue(sync());
  }

  private void syncAndVerify() throws Exception {

    final FsShell shell = new FsShell(conf);
    lsrSource("Before sync source: ", shell, source);
    lsr("Before sync target: ", shell, target);

    Assert.assertTrue(sync());

    lsrSource("After sync source: ", shell, source);
    lsr("After sync target: ", shell, target);

    verifyCopy(dfs.getFileStatus(source), dfs.getFileStatus(target), false);
  }

  private boolean sync() throws Exception {
    distCpContext = new DistCpContext(optionsBuilder.build());
    final DistCpSync distCpSync = new DistCpSync(distCpContext, conf);
    return distCpSync.sync();
  }

  private void enableAndCreateFirstSnapshot() throws Exception {
    if (isSrcNotSameAsTgt) {
      dfs.allowSnapshot(source);
      dfs.createSnapshot(source, "s1");
    }
    dfs.allowSnapshot(target);
    dfs.createSnapshot(target, "s1");
  }

  private void createSecondSnapshotAtTarget() throws Exception {
    dfs.createSnapshot(target, "s2");
  }

  private void createMiddleSnapshotAtTarget() throws Exception {
    dfs.createSnapshot(target, "s1.5");
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

    DFSTestUtil.createFile(dfs, f1, blockSize, dataNum, 0);
    DFSTestUtil.createFile(dfs, f2, blockSize, dataNum, 0);
    DFSTestUtil.createFile(dfs, f3, blockSize, dataNum, 0);
    DFSTestUtil.createFile(dfs, f4, blockSize, dataNum, 0);
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
    int numDeletedModified = 0;
    dfs.rename(d1, bar_d1);
    numDeletedModified += 1; // modify ./foo
    numDeletedModified += 1; // modify ./bar
    final Path f3 = new Path(bar_d1, "f3");
    dfs.delete(f3, true);
    numDeletedModified += 1; // delete f3
    final Path newfoo = new Path(bar_d1, "foo");
    dfs.rename(foo, newfoo);
    numDeletedModified += 1; // modify ./foo/d1
    final Path f1 = new Path(newfoo, "f1");
    dfs.delete(f1, true);
    numDeletedModified += 1; // delete ./foo/f1
    DFSTestUtil.createFile(dfs, f1, 2 * blockSize, dataNum, 0);
    DFSTestUtil.appendFile(dfs, f2, (int) blockSize);
    numDeletedModified += 1; // modify ./bar/f2
    dfs.rename(bar, new Path(dir, "foo"));
    return numDeletedModified;
  }

  /**
   * Test the basic functionality.
   */
  @Test
  public void testSync() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData(source);
    }
    initData(target);
    enableAndCreateFirstSnapshot();

    final FsShell shell = new FsShell(conf);

    lsrSource("Before source: ", shell, source);
    lsr("Before target: ", shell, target);

    // make changes under target
    int numDeletedModified = changeData(target);

    createSecondSnapshotAtTarget();

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(target, "s2", "s1");
    System.out.println(report);

    final DistCpSync distCpSync = new DistCpSync(distCpContext, conf);

    lsr("Before sync target: ", shell, target);

    // do the sync
    Assert.assertTrue(distCpSync.sync());

    lsr("After sync target: ", shell, target);

    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s1");
    Assert.assertEquals(spath, distCpContext.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new SimpleCopyListing(conf, new Credentials(),
        distCpSync);
    listing.buildListing(listingPath, distCpContext);

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

    lsrSource("After mapper source: ", shell, source);
    lsr("After mapper target: ", shell, target);

    // verify that we only list modified and created files/directories
    Assert.assertEquals(numDeletedModified, copyListing.size());

    // verify that we only copied new appended data of f2 and the new file f1
    Assert.assertEquals(blockSize * 3, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.BYTESCOPIED).getValue());

    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  private Map<Text, CopyListingFileStatus> getListing(Path listingPath)
      throws Exception {
    SequenceFile.Reader reader = null;
    Map<Text, CopyListingFileStatus> values = new HashMap<>();
    try {
      reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(listingPath));
      Text key = new Text();
      CopyListingFileStatus value = new CopyListingFileStatus();
      while (reader.next(key, value)) {
        values.put(key, value);
        key = new Text();
        value = new CopyListingFileStatus();
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
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
   * Test the case that "current" is snapshotted as "s2".
   * @throws Exception
   */
  @Test
  public void testSyncWithCurrent() throws Exception {
    optionsBuilder.withUseRdiff(".", "s1");
    if (isSrcNotSameAsTgt) {
      initData(source);
    }
    initData(target);
    enableAndCreateFirstSnapshot();

    // make changes under target
    changeData(target);

    // do the sync
    Assert.assertTrue(sync());
    final Path spath = new Path(source,
        HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s1");
    // make sure the source path is still unchanged
    Assert.assertEquals(spath, distCpContext.getSourcePaths().get(0));
  }

  private void initData2(Path dir) throws Exception {
    final Path test = new Path(dir, "test");
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path f1 = new Path(test, "f1");
    final Path f2 = new Path(foo, "f2");
    final Path f3 = new Path(bar, "f3");

    DFSTestUtil.createFile(dfs, f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, f2, blockSize, dataNum, 1L);
    DFSTestUtil.createFile(dfs, f3, blockSize, dataNum, 2L);
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
    if (isSrcNotSameAsTgt) {
      initData2(source);
    }
    initData2(target);
    enableAndCreateFirstSnapshot();

    // make changes under target
    changeData2(target);

    createSecondSnapshotAtTarget();

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(target, "s2", "s1");
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

    DFSTestUtil.createFile(dfs, f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, f2, blockSize * 2, dataNum, 1L);
    DFSTestUtil.createFile(dfs, f3, blockSize * 3, dataNum, 2L);
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
    if (isSrcNotSameAsTgt) {
      initData3(source);
    }
    initData3(target);
    enableAndCreateFirstSnapshot();

    // make changes under target
    changeData3(target);

    createSecondSnapshotAtTarget();

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(target, "s2", "s1");
    System.out.println(report);

    syncAndVerify();
  }

  private void initData4(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d2 = new Path(d1, "d2");
    final Path f1 = new Path(d2, "f1");

    DFSTestUtil.createFile(dfs, f1, blockSize, dataNum, 0L);
  }

  private int changeData4(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d11 = new Path(dir, "d11");
    final Path d2 = new Path(d1, "d2");
    final Path d21 = new Path(d1, "d21");
    final Path f1 = new Path(d2, "f1");

    int numDeletedAndModified = 0;
    dfs.delete(f1, false);
    numDeletedAndModified += 1;
    dfs.rename(d2, d21);
    numDeletedAndModified += 1;
    dfs.rename(d1, d11);
    numDeletedAndModified += 1;
    return numDeletedAndModified;
  }

  /**
   * Test a case where multiple level dirs are renamed.
   */
  @Test
  public void testSync4() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData4(source);
    }
    initData4(target);
    enableAndCreateFirstSnapshot();

    final FsShell shell = new FsShell(conf);
    lsr("Before change target: ", shell, target);

    // make changes under target
    int numDeletedAndModified = changeData4(target);

    createSecondSnapshotAtTarget();

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(target, "s2", "s1");
    System.out.println(report);

    testAndVerify(numDeletedAndModified);
  }

  private void initData5(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d2 = new Path(dir, "d2");
    final Path f1 = new Path(d1, "f1");
    final Path f2 = new Path(d2, "f2");

    DFSTestUtil.createFile(dfs, f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, f2, blockSize, dataNum, 0L);
  }

  private int changeData5(Path dir) throws Exception {
    final Path d1 = new Path(dir, "d1");
    final Path d2 = new Path(dir, "d2");
    final Path f1 = new Path(d1, "f1");
    final Path tmp = new Path(dir, "tmp");

    int numDeletedAndModified = 0;
    dfs.delete(f1, false);
    numDeletedAndModified += 1;
    dfs.rename(d1, tmp);
    numDeletedAndModified += 1;
    dfs.rename(d2, d1);
    numDeletedAndModified += 1;
    final Path f2 = new Path(d1, "f2");
    dfs.delete(f2, false);
    numDeletedAndModified += 1;
    return numDeletedAndModified;
  }

   /**
   * Test a case with different delete and rename sequences.
   */
  @Test
  public void testSync5() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData5(source);
    }
    initData5(target);
    enableAndCreateFirstSnapshot();

    // make changes under target
    int numDeletedAndModified = changeData5(target);

    createSecondSnapshotAtTarget();

    SnapshotDiffReport report = dfs.getSnapshotDiffReport(target, "s2", "s1");
    System.out.println(report);

    testAndVerify(numDeletedAndModified);
  }

  private void testAndVerify(int numDeletedAndModified)
          throws Exception{
    SnapshotDiffReport report = dfs.getSnapshotDiffReport(target, "s2", "s1");
    System.out.println(report);

    final FsShell shell = new FsShell(conf);

    lsrSource("Before sync source: ", shell, source);
    lsr("Before sync target: ", shell, target);

    DistCpSync distCpSync = new DistCpSync(distCpContext, conf);
    // do the sync
    distCpSync.sync();

    lsr("After sync target: ", shell, target);

    // make sure the source path has been updated to the snapshot path
    final Path spath = new Path(source,
            HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + "s1");
    Assert.assertEquals(spath, distCpContext.getSourcePaths().get(0));

    // build copy listing
    final Path listingPath = new Path("/tmp/META/fileList.seq");
    CopyListing listing = new SimpleCopyListing(conf, new Credentials(), distCpSync);
    listing.buildListing(listingPath, distCpContext);

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
    Assert.assertEquals(numDeletedAndModified, copyListing.size());

    lsr("After Copy target: ", shell, target);

    // verify the source and target now has the same structure
    verifyCopy(dfs.getFileStatus(spath), dfs.getFileStatus(target), false);
  }

  private void initData6(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path foo_f1 = new Path(foo, "f1");
    final Path bar_f1 = new Path(bar, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, bar_f1, blockSize, dataNum, 0L);
  }

  private int changeData6(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path foo2 = new Path(dir, "foo2");
    final Path foo_f1 = new Path(foo, "f1");

    int numDeletedModified = 0;
    dfs.rename(foo, foo2);
    dfs.rename(bar, foo);
    dfs.rename(foo2, bar);
    DFSTestUtil.appendFile(dfs, foo_f1, (int) blockSize);
    numDeletedModified += 1; // modify ./bar/f1
    return numDeletedModified;
  }

  /**
   * Test a case where there is a cycle in renaming dirs.
   */
  @Test
  public void testSync6() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData6(source);
    }
    initData6(target);
    enableAndCreateFirstSnapshot();
    int numDeletedModified = changeData6(target);

    createSecondSnapshotAtTarget();

    testAndVerify(numDeletedModified);
  }

  private void initData7(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path foo_f1 = new Path(foo, "f1");
    final Path bar_f1 = new Path(bar, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, bar_f1, blockSize, dataNum, 0L);
  }

  private int changeData7(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path foo2 = new Path(dir, "foo2");
    final Path foo_f1 = new Path(foo, "f1");
    final Path foo2_f2 = new Path(foo2, "f2");
    final Path foo_d1 = new Path(foo, "d1");
    final Path foo_d1_f3 = new Path(foo_d1, "f3");

    int numDeletedAndModified = 0;
    dfs.rename(foo, foo2);
    DFSTestUtil.createFile(dfs, foo_f1, blockSize, dataNum, 0L);
    DFSTestUtil.appendFile(dfs, foo_f1, (int) blockSize);
    dfs.rename(foo_f1, foo2_f2);
    /*
     * Difference between snapshot s1 and current directory under directory
       /target:
M       .
+       ./foo
R       ./foo -> ./foo2
M       ./foo
+       ./foo/f2
     */
    numDeletedAndModified += 1; // "M ./foo"
    DFSTestUtil.createFile(dfs, foo_d1_f3, blockSize, dataNum, 0L);
    return numDeletedAndModified;
  }

  /**
   * Test a case where rename a dir, then create a new dir with the same name
   * and sub dir.
   */
  @Test
  public void testSync7() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData7(source);
    }
    initData7(target);
    enableAndCreateFirstSnapshot();
    int numDeletedAndModified = changeData7(target);

    createSecondSnapshotAtTarget();

    testAndVerify(numDeletedAndModified);
  }

  private void initData8(Path dir) throws Exception {
    final Path foo = new Path(dir, "foo");
    final Path bar = new Path(dir, "bar");
    final Path d1 = new Path(dir, "d1");
    final Path foo_f1 = new Path(foo, "f1");
    final Path bar_f1 = new Path(bar, "f1");
    final Path d1_f1 = new Path(d1, "f1");

    DFSTestUtil.createFile(dfs, foo_f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, bar_f1, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, d1_f1, blockSize, dataNum, 0L);
  }

  private int changeData8(Path dir, boolean createMiddleSnapshot)
      throws Exception {
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

    int numDeletedAndModified = 0;
    DFSTestUtil.createFile(dfs, foo_f3, blockSize, dataNum, 0L);
    DFSTestUtil.createFile(dfs, createdDir_f1, blockSize, dataNum, 0L);
    dfs.rename(createdDir_f1, foo_f4);
    dfs.rename(d1_f1, createdDir_f1); // rename ./d1/f1 -> ./c/f1
    numDeletedAndModified += 1; // modify ./c/foo/d1

    if (createMiddleSnapshot) {
      this.createMiddleSnapshotAtTarget();
    }

    dfs.rename(d1, foo_d1);
    numDeletedAndModified += 1; // modify ./c/foo
    dfs.rename(foo, new_foo);
    dfs.rename(bar, bar1);
    return numDeletedAndModified;
  }

  /**
   * Test a case where create a dir, then mv a existed dir into it.
   */
  @Test
  public void testSync8() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData8(source);
    }
    initData8(target);
    enableAndCreateFirstSnapshot();
    int numDeletedModified = changeData8(target, false);

    createSecondSnapshotAtTarget();

    testAndVerify(numDeletedModified);
  }

  /**
   * Test a case where create a dir, then mv a existed dir into it.
   * The difference between this one and testSync8 is, this one
   * also creates a snapshot s1.5 in between s1 and s2.
   */
  @Test
  public void testSync9() throws Exception {
    if (isSrcNotSameAsTgt) {
      initData8(source);
    }
    initData8(target);
    enableAndCreateFirstSnapshot();
    int numDeletedModified = changeData8(target, true);

    createSecondSnapshotAtTarget();

    testAndVerify(numDeletedModified);
  }
}
