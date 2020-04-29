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

package org.apache.hadoop.fs.slive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.ArgumentParser.ParsedOutput;
import org.apache.hadoop.fs.slive.Constants.OperationType;
import org.apache.hadoop.fs.slive.DataVerifier.VerifyOutput;
import org.apache.hadoop.fs.slive.DataWriter.GenerateOutput;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Junit 4 test for slive
 */
public class TestSlive {

  private static final Logger LOG = LoggerFactory.getLogger(TestSlive.class);

  private static final Random rnd = new Random(1L);

  private static final String TEST_DATA_PROP = "test.build.data";

  private static Configuration getBaseConfig() {
    Configuration conf = new Configuration();
    return conf;
  }

  /** gets the test write location according to the coding guidelines */
  private static File getWriteLoc() {
    String writeLoc = System.getProperty(TEST_DATA_PROP, "build/test/data/");
    File writeDir = new File(writeLoc, "slive");
    writeDir.mkdirs();
    return writeDir;
  }

  /** gets where the MR job places its data + output + results */
  private static File getFlowLocation() {
    return new File(getWriteLoc(), "flow");
  }

  /** gets the test directory which is created by the mkdir op */
  private static File getTestDir() {
    return new File(getWriteLoc(), "slivedir");
  }

  /**
   * gets the test file location
   * which is used for reading, appending and created
   */
  private static File getTestFile() {
    return new File(getWriteLoc(), "slivefile");
  }

  /**
   * gets the rename file which is used in combination
   * with the test file to do a rename operation
   */
  private static File getTestRenameFile() {
    return new File(getWriteLoc(), "slivefile1");
  }

  /** gets the MR result file name */
  private static File getResultFile() {
    return new File(getWriteLoc(), "sliveresfile");
  }

  private static File getImaginaryFile() {
    return new File(getWriteLoc(), "slivenofile");
  }

  /** gets the test program arguments used for merging and main MR running */
  private String[] getTestArgs(boolean sleep) {
    List<String> args = new LinkedList<String>();
    // setup the options
    {
      args.add("-" + ConfigOption.WRITE_SIZE.getOpt());
      args.add("1M,2M");
      args.add("-" + ConfigOption.OPS.getOpt());
      args.add(Constants.OperationType.values().length + "");
      args.add("-" + ConfigOption.MAPS.getOpt());
      args.add("2");
      args.add("-" + ConfigOption.REDUCES.getOpt());
      args.add("2");
      args.add("-" + ConfigOption.APPEND_SIZE.getOpt());
      args.add("1M,2M");
      args.add("-" + ConfigOption.BLOCK_SIZE.getOpt());
      args.add("1M,2M");
      args.add("-" + ConfigOption.REPLICATION_AM.getOpt());
      args.add("1,1");
      if (sleep) {
        args.add("-" + ConfigOption.SLEEP_TIME.getOpt());
        args.add("10,10");
      }
      args.add("-" + ConfigOption.RESULT_FILE.getOpt());
      args.add(getResultFile().toString());
      args.add("-" + ConfigOption.BASE_DIR.getOpt());
      args.add(getFlowLocation().toString());
      args.add("-" + ConfigOption.DURATION.getOpt());
      args.add("10");
      args.add("-" + ConfigOption.DIR_SIZE.getOpt());
      args.add("10");
      args.add("-" + ConfigOption.FILES.getOpt());
      args.add("10");
      args.add("-" + ConfigOption.TRUNCATE_SIZE.getOpt());
      args.add("0,1M");
    }
    return args.toArray(new String[args.size()]);
  }

  @Test
  public void testFinder() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    PathFinder fr = new PathFinder(extractor, rnd);
    // should only be able to select 10 files
    // attempt for a given amount of iterations
    int maxIterations = 10000;
    Set<Path> files = new HashSet<Path>();
    for (int i = 0; i < maxIterations; i++) {
      files.add(fr.getFile());
    }
    assertTrue(files.size() == 10);
    Set<Path> dirs = new HashSet<Path>();
    for (int i = 0; i < maxIterations; i++) {
      dirs.add(fr.getDirectory());
    }
    assertTrue(dirs.size() == 10);
  }

  @Test
  public void testSelection() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    WeightSelector selector = new WeightSelector(extractor, rnd);
    // should be 1 of each type - uniform
    int expected = OperationType.values().length;
    Operation op = null;
    Set<String> types = new HashSet<String>();
    FileSystem fs = FileSystem.get(extractor.getConfig());
    while (true) {
      op = selector.select(1, 1);
      if (op == null) {
        break;
      }
      // doesn't matter if they work or not
      op.run(fs);
      types.add(op.getType());
    }
    assertThat(types.size()).isEqualTo(expected);
  }

  // gets the config merged with the arguments
  private ConfigExtractor getTestConfig(boolean sleep) throws Exception {
    ArgumentParser parser = new ArgumentParser(getTestArgs(sleep));
    ParsedOutput out = parser.parse();
    assertTrue(!out.shouldOutputHelp());
    ConfigMerger merge = new ConfigMerger();
    Configuration cfg = merge.getMerged(out, getBaseConfig());
    ConfigExtractor extractor = new ConfigExtractor(cfg);
    return extractor;
  }

  @Before
  public void ensureDeleted() throws Exception {
    rDelete(getTestFile());
    rDelete(getTestDir());
    rDelete(getTestRenameFile());
    rDelete(getResultFile());
    rDelete(getFlowLocation());
    rDelete(getImaginaryFile());
  }

  /** cleans up a file or directory recursively if need be */
  private void rDelete(File place) throws Exception {
    if (place.isFile()) {
      LOG.info("Deleting file " + place);
      assertTrue(place.delete());
    } else if (place.isDirectory()) {
      deleteDir(place);
    }
  }

  /** deletes a dir and its contents */
  private void deleteDir(File dir) throws Exception {
    String fns[] = dir.list();
    // delete contents first
    for (String afn : fns) {
      File fn = new File(dir, afn);
      rDelete(fn);
    }
    LOG.info("Deleting directory " + dir);
    // now delete the dir
    assertTrue(dir.delete());
  }

  @Test
  public void testArguments() throws Exception {
    ConfigExtractor extractor = getTestConfig(true);
    assertEquals(extractor.getOpCount().intValue(), Constants.OperationType
        .values().length);
    assertThat(extractor.getMapAmount().intValue()).isEqualTo(2);
    assertThat(extractor.getReducerAmount().intValue()).isEqualTo(2);
    Range<Long> apRange = extractor.getAppendSize();
    assertThat(apRange.getLower().intValue()).isEqualTo(
        Constants.MEGABYTES * 1);
    assertThat(apRange.getUpper().intValue()).isEqualTo(
        Constants.MEGABYTES * 2);
    Range<Long> wRange = extractor.getWriteSize();
    assertThat(wRange.getLower().intValue()).isEqualTo(
        Constants.MEGABYTES * 1);
    assertThat(wRange.getUpper().intValue()).isEqualTo(
        Constants.MEGABYTES * 2);
    Range<Long> trRange = extractor.getTruncateSize();
    assertThat(trRange.getLower().intValue()).isZero();
    assertThat(trRange.getUpper().intValue()).isEqualTo(
        Constants.MEGABYTES * 1);
    Range<Long> bRange = extractor.getBlockSize();
    assertThat(bRange.getLower().intValue()).isEqualTo(
        Constants.MEGABYTES * 1);
    assertThat(bRange.getUpper().intValue()).isEqualTo(
        Constants.MEGABYTES * 2);
    String resfile = extractor.getResultFile();
    assertEquals(resfile, getResultFile().toString());
    int durationMs = extractor.getDurationMilliseconds();
    assertThat(durationMs).isEqualTo(10 * 1000);
  }

  @Test
  public void testDataWriting() throws Exception {
    long byteAm = 100;
    File fn = getTestFile();
    DataWriter writer = new DataWriter(rnd);
    FileOutputStream fs = new FileOutputStream(fn);
    GenerateOutput ostat = writer.writeSegment(byteAm, fs);
    LOG.info(ostat.toString());
    fs.close();
    assertTrue(ostat.getBytesWritten() == byteAm);
    DataVerifier vf = new DataVerifier();
    FileInputStream fin = new FileInputStream(fn);
    VerifyOutput vfout = vf.verifyFile(byteAm, new DataInputStream(fin));
    LOG.info(vfout.toString());
    fin.close();
    assertEquals(vfout.getBytesRead(), byteAm);
    assertTrue(vfout.getChunksDifferent() == 0);
  }

  @Test
  public void testRange() {
    Range<Long> r = new Range<Long>(10L, 20L);
    assertThat(r.getLower().longValue()).isEqualTo(10L);
    assertThat(r.getUpper().longValue()).isEqualTo(20L);
  }

  @Test
  public void testCreateOp() throws Exception {
    // setup a valid config
    ConfigExtractor extractor = getTestConfig(false);
    final Path fn = new Path(getTestFile().getCanonicalPath());
    CreateOp op = new CreateOp(extractor, rnd) {
      protected Path getCreateFile() {
        return fn;
      }
    };
    runOperationOk(extractor, op, true);
  }

  @Test
  public void testOpFailures() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    final Path fn = new Path(getImaginaryFile().getCanonicalPath());
    ReadOp rop = new ReadOp(extractor, rnd) {
      protected Path getReadFile() {
        return fn;
      }
    };
    runOperationBad(extractor, rop);

    DeleteOp dop = new DeleteOp(extractor, rnd) {
      protected Path getDeleteFile() {
        return fn;
      }
    };
    runOperationBad(extractor, dop);

    RenameOp reop = new RenameOp(extractor, rnd) {
      protected SrcTarget getRenames() {
        return new SrcTarget(fn, fn);
      }
    };
    runOperationBad(extractor, reop);

    AppendOp aop = new AppendOp(extractor, rnd) {
      protected Path getAppendFile() {
        return fn;
      }
    };

    runOperationBad(extractor, aop);
  }

  private void runOperationBad(ConfigExtractor cfg, Operation op)
      throws Exception {
    FileSystem fs = FileSystem.get(cfg.getConfig());
    List<OperationOutput> data = op.run(fs);
    assertTrue(!data.isEmpty());
    boolean foundFail = false;
    for (OperationOutput d : data) {
      if (d.getMeasurementType().equals(ReportWriter.FAILURES)) {
        foundFail = true;
      }
      if (d.getMeasurementType().equals(ReportWriter.NOT_FOUND)) {
        foundFail = true;
      }
    }
    assertTrue(foundFail);
  }

  private void runOperationOk(ConfigExtractor cfg, Operation op, boolean checkOk)
      throws Exception {
    FileSystem fs = FileSystem.get(cfg.getConfig());
    List<OperationOutput> data = op.run(fs);
    assertTrue(!data.isEmpty());
    if (checkOk) {
      boolean foundSuc = false;
      boolean foundOpCount = false;
      boolean foundTime = false;
      for (OperationOutput d : data) {
        assertTrue(!d.getMeasurementType().equals(ReportWriter.FAILURES));
        if (d.getMeasurementType().equals(ReportWriter.SUCCESSES)) {
          foundSuc = true;
        }
        if (d.getMeasurementType().equals(ReportWriter.OP_COUNT)) {
          foundOpCount = true;
        }
        if (d.getMeasurementType().equals(ReportWriter.OK_TIME_TAKEN)) {
          foundTime = true;
        }
      }
      assertTrue(foundSuc);
      assertTrue(foundOpCount);
      assertTrue(foundTime);
    }
  }

  @Test
  public void testDelete() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    final Path fn = new Path(getTestFile().getCanonicalPath());
    // ensure file created before delete
    CreateOp op = new CreateOp(extractor, rnd) {
      protected Path getCreateFile() {
        return fn;
      }
    };
    runOperationOk(extractor, op, true);
    // now delete
    DeleteOp dop = new DeleteOp(extractor, rnd) {
      protected Path getDeleteFile() {
        return fn;
      }
    };
    runOperationOk(extractor, dop, true);
  }

  @Test
  public void testRename() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    final Path src = new Path(getTestFile().getCanonicalPath());
    final Path tgt = new Path(getTestRenameFile().getCanonicalPath());
    // ensure file created before rename
    CreateOp op = new CreateOp(extractor, rnd) {
      protected Path getCreateFile() {
        return src;
      }
    };
    runOperationOk(extractor, op, true);
    RenameOp rop = new RenameOp(extractor, rnd) {
      protected SrcTarget getRenames() {
        return new SrcTarget(src, tgt);
      }
    };
    runOperationOk(extractor, rop, true);
  }

  @Test
  public void testMRFlow() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    SliveTest s = new SliveTest(getBaseConfig());
    int ec = ToolRunner.run(s, getTestArgs(false));
    assertTrue(ec == 0);
    String resFile = extractor.getResultFile();
    File fn = new File(resFile);
    assertTrue(fn.exists());
    // can't validate completely since operations may fail (mainly anyone but
    // create +mkdir) since they may not find there files
  }

  @Test
  public void testRead() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    final Path fn = new Path(getTestFile().getCanonicalPath());
    // ensure file created before read
    CreateOp op = new CreateOp(extractor, rnd) {
      protected Path getCreateFile() {
        return fn;
      }
    };
    runOperationOk(extractor, op, true);
    ReadOp rop = new ReadOp(extractor, rnd) {
      protected Path getReadFile() {
        return fn;
      }
    };
    runOperationOk(extractor, rop, true);
  }

  @Test
  public void testSleep() throws Exception {
    ConfigExtractor extractor = getTestConfig(true);
    SleepOp op = new SleepOp(extractor, rnd);
    runOperationOk(extractor, op, true);
  }

  @Test
  public void testList() throws Exception {
    // ensure dir made
    ConfigExtractor extractor = getTestConfig(false);
    final Path dir = new Path(getTestDir().getCanonicalPath());
    MkdirOp op = new MkdirOp(extractor, rnd) {
      protected Path getDirectory() {
        return dir;
      }
    };
    runOperationOk(extractor, op, true);
    // list it
    ListOp lop = new ListOp(extractor, rnd) {
      protected Path getDirectory() {
        return dir;
      }
    };
    runOperationOk(extractor, lop, true);
  }

  @Test
  public void testBadChunks() throws Exception {
    File fn = getTestFile();
    int byteAm = 10000;
    FileOutputStream fout = new FileOutputStream(fn);
    byte[] bytes = new byte[byteAm];
    rnd.nextBytes(bytes);
    fout.write(bytes);
    fout.close();
    // attempt to read it
    DataVerifier vf = new DataVerifier();
    VerifyOutput vout = new VerifyOutput(0, 0, 0, 0);
    DataInputStream in = null;
    try {
      in = new DataInputStream(new FileInputStream(fn));
      vout = vf.verifyFile(byteAm, in);
    } catch (Exception e) {

    } finally {
      if(in != null) in.close();
    }
    assertTrue(vout.getChunksSame() == 0);
  }

  @Test
  public void testMkdir() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    final Path dir = new Path(getTestDir().getCanonicalPath());
    MkdirOp op = new MkdirOp(extractor, rnd) {
      protected Path getDirectory() {
        return dir;
      }
    };
    runOperationOk(extractor, op, true);
  }

  @Test
  public void testSelector() throws Exception {
    ConfigExtractor extractor = getTestConfig(false);
    RouletteSelector selector = new RouletteSelector(rnd);
    List<OperationWeight> sList = new LinkedList<OperationWeight>();
    Operation op = selector.select(sList);
    assertTrue(op == null);
    CreateOp cop = new CreateOp(extractor, rnd);
    sList.add(new OperationWeight(cop, 1.0d));
    AppendOp aop = new AppendOp(extractor, rnd);
    sList.add(new OperationWeight(aop, 0.01d));
    op = selector.select(sList);
    assertTrue(op == cop);
  }

  @Test
  public void testAppendOp() throws Exception {
    // setup a valid config
    ConfigExtractor extractor = getTestConfig(false);
    // ensure file created before append
    final Path fn = new Path(getTestFile().getCanonicalPath());
    CreateOp op = new CreateOp(extractor, rnd) {
      protected Path getCreateFile() {
        return fn;
      }
    };
    runOperationOk(extractor, op, true);
    // local file system (ChecksumFileSystem) currently doesn't support append -
    // but we'll leave this test here anyways but can't check the results..
    AppendOp aop = new AppendOp(extractor, rnd) {
      protected Path getAppendFile() {
        return fn;
      }
    };
    runOperationOk(extractor, aop, false);
  }

  @Test
  public void testTruncateOp() throws Exception {
    // setup a valid config
    ConfigExtractor extractor = getTestConfig(false);
    // ensure file created before append
    final Path fn = new Path(getTestFile().getCanonicalPath());
    CreateOp op = new CreateOp(extractor, rnd) {
      protected Path getCreateFile() {
        return fn;
      }
    };
    runOperationOk(extractor, op, true);
    // local file system (ChecksumFileSystem) currently doesn't support truncate -
    // but we'll leave this test here anyways but can't check the results..
    TruncateOp top = new TruncateOp(extractor, rnd) {
      protected Path getTruncateFile() {
        return fn;
      }
    };
    runOperationOk(extractor, top, false);
  }
}
