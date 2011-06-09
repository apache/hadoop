/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * Testing {@link HLog} splitting code.
 */
public class TestHLogSplit {

  private final static Log LOG = LogFactory.getLog(TestHLogSplit.class);

  private Configuration conf;
  private FileSystem fs;

  private final static HBaseTestingUtility
          TEST_UTIL = new HBaseTestingUtility();


  private static final Path hbaseDir = new Path("/hbase");
  private static final Path hlogDir = new Path(hbaseDir, "hlog");
  private static final Path oldLogDir = new Path(hbaseDir, "hlog.old");
  private static final Path corruptDir = new Path(hbaseDir, ".corrupt");

  private static final int NUM_WRITERS = 10;
  private static final int ENTRIES = 10; // entries per writer per region

  private HLog.Writer[] writer = new HLog.Writer[NUM_WRITERS];
  private long seq = 0;
  private static final byte[] TABLE_NAME = "t1".getBytes();
  private static final byte[] FAMILY = "f1".getBytes();
  private static final byte[] QUALIFIER = "q1".getBytes();
  private static final byte[] VALUE = "v1".getBytes();
  private static final String HLOG_FILE_PREFIX = "hlog.dat.";
  private static List<String> regions;
  private static final String HBASE_SKIP_ERRORS = "hbase.hlog.split.skip.errors";
  private static final Path tabledir =
      new Path(hbaseDir, Bytes.toString(TABLE_NAME));

  static enum Corruptions {
    INSERT_GARBAGE_ON_FIRST_LINE,
    INSERT_GARBAGE_IN_THE_MIDDLE,
    APPEND_GARBAGE,
    TRUNCATE,
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().
            setBoolean("dfs.support.append", true);
    TEST_UTIL.getConfiguration().
            setStrings("hbase.rootdir", hbaseDir.toString());
    TEST_UTIL.getConfiguration().
            setClass("hbase.regionserver.hlog.writer.impl",
                InstrumentedSequenceFileLogWriter.class, HLog.Writer.class);

    TEST_UTIL.startMiniDFSCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Before
  public void setUp() throws Exception {
    flushToConsole("Cleaning up cluster for new test\n"
        + "--------------------------");
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    FileStatus[] entries = fs.listStatus(new Path("/"));
    flushToConsole("Num entries in /:" + entries.length);
    for (FileStatus dir : entries){
      assertTrue("Deleting " + dir.getPath(),
          fs.delete(dir.getPath(), true));
    }
    seq = 0;
    regions = new ArrayList<String>();
    Collections.addAll(regions, "bbb", "ccc");
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    // Set the soft lease for hdfs to be down from default of 5 minutes or so.
    TEST_UTIL.setNameNodeNameSystemLeasePeriod(100, 50000);
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * @throws IOException
   * @see https://issues.apache.org/jira/browse/HBASE-3020
   */
  @Test public void testRecoveredEditsPathForMeta() throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = new Path(hbaseDir, Bytes.toString(HConstants.META_TABLE_NAME));
    Path regiondir = new Path(tdir,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    fs.mkdirs(regiondir);
    long now = System.currentTimeMillis();
    HLog.Entry entry =
      new HLog.Entry(new HLogKey(encoded, HConstants.META_TABLE_NAME, 1, now),
      new WALEdit());
    Path p = HLogSplitter.getRegionSplitEditsPath(fs, entry, hbaseDir, true);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
  }

  @Test(expected = OrphanHLogAfterSplitException.class)
  public void testSplitFailsIfNewHLogGetsCreatedAfterSplitStarted()
  throws IOException {
    AtomicBoolean stop = new AtomicBoolean(false);

    assertFalse("Previous test should clean up table dir",
      fs.exists(new Path("/hbase/t1")));

    generateHLogs(-1);

    try {
    (new ZombieNewLogWriterRegionServer(stop)).start();
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();
    } finally {
      stop.set(true);
    }
  }

  @Test
  public void testSplitPreservesEdits() throws IOException{
    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);

    generateHLogs(1, 10, -1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
      hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, REGION);

    assertEquals("edits differ after split", true, logsAreEqual(originalLog, splitLog));
  }


  @Test
  public void testEmptyLogFiles() throws IOException {

    injectEmptyFile(".empty", true);
    generateHLogs(Integer.MAX_VALUE);
    injectEmptyFile("empty", true);

    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();


    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }

  }


  @Test
  public void testEmptyOpenLogFiles() throws IOException {
    injectEmptyFile(".empty", false);
    generateHLogs(Integer.MAX_VALUE);
    injectEmptyFile("empty", false);

    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }
  }

  @Test
  public void testOpenZeroLengthReportedFileButWithDataGetsSplit() throws IOException {
    // generate logs but leave hlog.dat.5 open.
    generateHLogs(5);

    fs.initialize(fs.getUri(), conf);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }


  }


  @Test
  public void testTralingGarbageCorruptionFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);
    fs.initialize(fs.getUri(), conf);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();
    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }


  }

  @Test
  public void testFirstLineCorruptionLogFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);
    fs.initialize(fs.getUri(), conf);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();
    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals((NUM_WRITERS - 1) * ENTRIES, countHLog(logfile, fs, conf));
    }


  }


  @Test
  public void testMiddleGarbageCorruptionSkipErrorsReadsHalfOfFile() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false, fs);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      // the entries in the original logs are alternating regions
      // considering the sequence file header, the middle corruption should
      // affect at least half of the entries
      int goodEntries = (NUM_WRITERS - 1) * ENTRIES;
      int firstHalfEntries = (int) Math.ceil(ENTRIES / 2) - 1;
      assertTrue("The file up to the corrupted area hasn't been parsed",
              goodEntries + firstHalfEntries <= countHLog(logfile, fs, conf));
    }
  }

  @Test
  public void testCorruptedFileGetsArchivedIfSkipErrors() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    HLog.resetLogReaderClass();

    try {
    Path c1 = new Path(hlogDir, HLOG_FILE_PREFIX + "0");
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, HLog.Reader.class);
      for (FaultySequenceFileLogReader.FailureType  failureType : FaultySequenceFileLogReader.FailureType.values()) {
        conf.set("faultysequencefilelogreader.failuretype", failureType.name());
        generateHLogs(1, ENTRIES, -1);
        fs.initialize(fs.getUri(), conf);
        HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
            hbaseDir, hlogDir, oldLogDir, fs);
        logSplitter.splitLog();
        FileStatus[] archivedLogs = fs.listStatus(corruptDir);
        assertEquals("expected a different file", c1.getName(), archivedLogs[0]
            .getPath().getName());
        assertEquals(archivedLogs.length, 1);
        fs.delete(new Path(oldLogDir, HLOG_FILE_PREFIX + "0"), false);
      }
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
      HLog.resetLogReaderClass();
    }
  }

  @Test(expected = IOException.class)
  public void testTrailingGarbageCorruptionLogFileSkipErrorsFalseThrows()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    HLog.resetLogReaderClass();

    try {
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, HLog.Reader.class);
      conf.set("faultysequencefilelogreader.failuretype", FaultySequenceFileLogReader.FailureType.BEGINNING.name());
      generateHLogs(Integer.MAX_VALUE);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
      HLog.resetLogReaderClass();
    }

  }

  @Test
  public void testCorruptedLogFilesSkipErrorsFalseDoesNotTouchLogs()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    HLog.resetLogReaderClass();

    try {
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, HLog.Reader.class);
      conf.set("faultysequencefilelogreader.failuretype", FaultySequenceFileLogReader.FailureType.BEGINNING.name());
      generateHLogs(-1);
      fs.initialize(fs.getUri(), conf);
      HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
          hbaseDir, hlogDir, oldLogDir, fs);
      try {
        logSplitter.splitLog();
      } catch (IOException e) {
        assertEquals(
            "if skip.errors is false all files should remain in place",
            NUM_WRITERS, fs.listStatus(hlogDir).length);
      }
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
      HLog.resetLogReaderClass();
    }

  }

  @Test
  public void testEOFisIgnored() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);

    int entryCount = 10;
    Path c1 = new Path(hlogDir, HLOG_FILE_PREFIX + "0");
    generateHLogs(1, entryCount, -1);
    corruptHLog(c1, Corruptions.TRUNCATE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, REGION);

    int actualCount = 0;
    HLog.Reader in = HLog.getReader(fs, splitLog, conf);
    HLog.Entry entry;
    while ((entry = in.next()) != null) ++actualCount;
    assertEquals(entryCount-1, actualCount);

    // should not have stored the EOF files as corrupt
    FileStatus[] archivedLogs = fs.listStatus(corruptDir);
    assertEquals(archivedLogs.length, 0);
  }

  @Test
  public void testLogsGetArchivedAfterSplit() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    FileStatus[] archivedLogs = fs.listStatus(oldLogDir);

    assertEquals("wrong number of files in the archive log", NUM_WRITERS, archivedLogs.length);
  }

  @Test
  public void testSplit() throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));

    }
  }

  @Test
  public void testLogDirectoryShouldBeDeletedAfterSuccessfulSplit()
  throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();
    FileStatus [] statuses = null;
    try {
      statuses = fs.listStatus(hlogDir);
      if (statuses != null) {
        Assert.fail("Files left in log dir: " +
            Joiner.on(",").join(FileUtil.stat2Paths(statuses)));
      }
    } catch (FileNotFoundException e) {
      // hadoop 0.21 throws FNFE whereas hadoop 0.20 returns null
    }
  }
/* DISABLED for now.  TODO: HBASE-2645
  @Test
  public void testLogCannotBeWrittenOnceParsed() throws IOException {
    AtomicLong counter = new AtomicLong(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    generateHLogs(9);
    fs.initialize(fs.getUri(), conf);

    Thread zombie = new ZombieLastLogWriterRegionServer(writer[9], counter, stop);



    try {
      zombie.start();

      HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, "juliet");

      // It's possible that the writer got an error while appending and didn't count it
      // however the entry will in fact be written to file and split with the rest
      long numberOfEditsInRegion = countHLog(logfile, fs, conf);
      assertTrue("The log file could have at most 1 extra log entry, but " +
              "can't have less. Zombie could write "+counter.get() +" and logfile had only"+ numberOfEditsInRegion+" "  + logfile, counter.get() == numberOfEditsInRegion ||
                      counter.get() + 1 == numberOfEditsInRegion);
    } finally {
      stop.set(true);
    }
  }
*/

  @Test
  public void testSplitWillNotTouchLogsIfNewHLogGetsCreatedAfterSplitStarted()
  throws IOException {
    AtomicBoolean stop = new AtomicBoolean(false);
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    Thread zombie = new ZombieNewLogWriterRegionServer(stop);

    try {
      zombie.start();
      try {
        HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
            hbaseDir, hlogDir, oldLogDir, fs);
        logSplitter.splitLog();
      } catch (IOException ex) {/* expected */}
      int logFilesNumber = fs.listStatus(hlogDir).length;

      assertEquals("Log files should not be archived if there's an extra file after split",
              NUM_WRITERS + 1, logFilesNumber);
    } finally {
      stop.set(true);
    }

  }



  @Test(expected = IOException.class)
  public void testSplitWillFailIfWritingToRegionFails() throws Exception {
    //leave 5th log open so we could append the "trap"
    generateHLogs(4);

    fs.initialize(fs.getUri(), conf);

    String region = "break";
    Path regiondir = new Path(tabledir, region);
    fs.mkdirs(regiondir);

    InstrumentedSequenceFileLogWriter.activateFailure = false;
    appendEntry(writer[4], TABLE_NAME, Bytes.toBytes(region),
        ("r" + 999).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
    writer[4].close();

    try {
      InstrumentedSequenceFileLogWriter.activateFailure = true;
      HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
          hbaseDir, hlogDir, oldLogDir, fs);
      logSplitter.splitLog();

    } catch (IOException e) {
      assertEquals("This exception is instrumented and should only be thrown for testing", e.getMessage());
      throw e;
    } finally {
      InstrumentedSequenceFileLogWriter.activateFailure = false;
    }
  }


  // @Test TODO this test has been disabled since it was created!
  // It currently fails because the second split doesn't output anything
  // -- because there are no region dirs after we move aside the first
  // split result
  public void testSplittingLargeNumberOfRegionsConsistency() throws IOException {

    regions.removeAll(regions);
    for (int i=0; i<100; i++) {
      regions.add("region__"+i);
    }

    generateHLogs(1, 100, -1);
    fs.initialize(fs.getUri(), conf);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();
    fs.rename(oldLogDir, hlogDir);
    Path firstSplitPath = new Path(hbaseDir, Bytes.toString(TABLE_NAME) + ".first");
    Path splitPath = new Path(hbaseDir, Bytes.toString(TABLE_NAME));
    fs.rename(splitPath,
            firstSplitPath);


    fs.initialize(fs.getUri(), conf);
    logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    assertEquals(0, compareHLogSplitDirs(firstSplitPath, splitPath));
  }

  @Test
  public void testSplitDeletedRegion() throws IOException {
    regions.removeAll(regions);
    String region = "region_that_splits";
    regions.add(region);

    generateHLogs(1);

    fs.initialize(fs.getUri(), conf);

    Path regiondir = new Path(tabledir, region);
    fs.delete(regiondir, true);

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(conf,
        hbaseDir, hlogDir, oldLogDir, fs);
    logSplitter.splitLog();

    assertFalse(fs.exists(regiondir));
  }

  @Test
  public void testIOEOnOutputThread() throws Exception {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);
    // Set up a splitter that will throw an IOE on the output side
    HLogSplitter logSplitter = new HLogSplitter(
        conf, hbaseDir, hlogDir, oldLogDir, fs) {
      protected HLog.Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
        HLog.Writer mockWriter = Mockito.mock(HLog.Writer.class);
        Mockito.doThrow(new IOException("Injected")).when(mockWriter).append(Mockito.<HLog.Entry>any());
        return mockWriter;

      }
    };
    try {
      logSplitter.splitLog();
      fail("Didn't throw!");
    } catch (IOException ioe) {
      assertTrue(ioe.toString().contains("Injected"));
    }
  }

  // Test for HBASE-3412
  @Test
  public void testMovedHLogDuringRecovery() throws Exception {
    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);

    // This partial mock will throw LEE for every file simulating
    // files that were moved
    FileSystem spiedFs = Mockito.spy(fs);
    // The "File does not exist" part is very important,
    // that's how it comes out of HDFS
    Mockito.doThrow(new LeaseExpiredException("Injected: File does not exist")).
        when(spiedFs).append(Mockito.<Path>any());

    HLogSplitter logSplitter = new HLogSplitter(
        conf, hbaseDir, hlogDir, oldLogDir, spiedFs);

    try {
      logSplitter.splitLog();
      assertEquals(NUM_WRITERS, fs.listStatus(oldLogDir).length);
      assertFalse(fs.exists(hlogDir));
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    }
  }

  /**
   * Test log split process with fake data and lots of edits to trigger threading
   * issues.
   */
  @Test
  public void testThreading() throws Exception {
    doTestThreading(20000, 128*1024*1024, 0);
  }

  /**
   * Test blocking behavior of the log split process if writers are writing slower
   * than the reader is reading.
   */
  @Test
  public void testThreadingSlowWriterSmallBuffer() throws Exception {
    doTestThreading(200, 1024, 50);
  }

  /**
   * Sets up a log splitter with a mock reader and writer. The mock reader generates
   * a specified number of edits spread across 5 regions. The mock writer optionally
   * sleeps for each edit it is fed.
   * *
   * After the split is complete, verifies that the statistics show the correct number
   * of edits output into each region.
   *
   * @param numFakeEdits number of fake edits to push through pipeline
   * @param bufferSize size of in-memory buffer
   * @param writerSlowness writer threads will sleep this many ms per edit
   */
  private void doTestThreading(final int numFakeEdits,
      final int bufferSize,
      final int writerSlowness) throws Exception {

    Configuration localConf = new Configuration(conf);
    localConf.setInt("hbase.regionserver.hlog.splitlog.buffersize", bufferSize);

    // Create a fake log file (we'll override the reader to produce a stream of edits)
    FSDataOutputStream out = fs.create(new Path(hlogDir, HLOG_FILE_PREFIX + ".fake"));
    out.close();

    // Make region dirs for our destination regions so the output doesn't get skipped
    final List<String> regions = ImmutableList.of("r0", "r1", "r2", "r3", "r4");
    makeRegionDirs(fs, regions);

    // Create a splitter that reads and writes the data without touching disk
    HLogSplitter logSplitter = new HLogSplitter(
        localConf, hbaseDir, hlogDir, oldLogDir, fs) {

      /* Produce a mock writer that doesn't write anywhere */
      protected HLog.Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
        HLog.Writer mockWriter = Mockito.mock(HLog.Writer.class);
        Mockito.doAnswer(new Answer<Void>() {
          int expectedIndex = 0;

          @Override
          public Void answer(InvocationOnMock invocation) {
            if (writerSlowness > 0) {
              try {
                Thread.sleep(writerSlowness);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
            }
            HLog.Entry entry = (Entry) invocation.getArguments()[0];
            WALEdit edit = entry.getEdit();
            List<KeyValue> keyValues = edit.getKeyValues();
            assertEquals(1, keyValues.size());
            KeyValue kv = keyValues.get(0);

            // Check that the edits come in the right order.
            assertEquals(expectedIndex, Bytes.toInt(kv.getRow()));
            expectedIndex++;
            return null;
          }
        }).when(mockWriter).append(Mockito.<HLog.Entry>any());
        return mockWriter;
      }


      /* Produce a mock reader that generates fake entries */
      protected Reader getReader(FileSystem fs, Path curLogFile, Configuration conf)
      throws IOException {
        Reader mockReader = Mockito.mock(Reader.class);
        Mockito.doAnswer(new Answer<HLog.Entry>() {
          int index = 0;

          @Override
          public HLog.Entry answer(InvocationOnMock invocation) throws Throwable {
            if (index >= numFakeEdits) return null;

            // Generate r0 through r4 in round robin fashion
            int regionIdx = index % regions.size();
            byte region[] = new byte[] {(byte)'r', (byte) (0x30 + regionIdx)};

            HLog.Entry ret = createTestEntry(TABLE_NAME, region,
                Bytes.toBytes((int)(index / regions.size())),
                FAMILY, QUALIFIER, VALUE, index);
            index++;
            return ret;
          }
        }).when(mockReader).next();
        return mockReader;
      }
    };

    logSplitter.splitLog();

    // Verify number of written edits per region

    Map<byte[], Long> outputCounts = logSplitter.getOutputCounts();
    for (Map.Entry<byte[], Long> entry : outputCounts.entrySet()) {
      LOG.info("Got " + entry.getValue() + " output edits for region " +
          Bytes.toString(entry.getKey()));

      assertEquals((long)entry.getValue(), numFakeEdits / regions.size());
    }
    assertEquals(regions.size(), outputCounts.size());
  }



  /**
   * This thread will keep writing to the file after the split process has started
   * It simulates a region server that was considered dead but woke up and wrote
   * some more to he last log entry
   */
  class ZombieLastLogWriterRegionServer extends Thread {
    AtomicLong editsCount;
    AtomicBoolean stop;
    Path log;
    HLog.Writer lastLogWriter;
    public ZombieLastLogWriterRegionServer(HLog.Writer writer, AtomicLong counter, AtomicBoolean stop) {
      this.stop = stop;
      this.editsCount = counter;
      this.lastLogWriter = writer;
    }

    @Override
    public void run() {
      if (stop.get()){
        return;
      }
      flushToConsole("starting");
      while (true) {
        try {
          String region = "juliet";

          fs.mkdirs(new Path(new Path(hbaseDir, region), region));
          appendEntry(lastLogWriter, TABLE_NAME, region.getBytes(),
                  ("r" + editsCount).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
          lastLogWriter.sync();
          editsCount.incrementAndGet();
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            //
          }


        } catch (IOException ex) {
          if (ex instanceof RemoteException) {
            flushToConsole("Juliet: got RemoteException " +
                    ex.getMessage() + " while writing " + (editsCount.get() + 1));
            break;
          } else {
            assertTrue("Failed to write " + editsCount.get(), false);
          }

        }
      }


    }
  }

  /**
   * This thread will keep adding new log files
   * It simulates a region server that was considered dead but woke up and wrote
   * some more to a new hlog
   */
  class ZombieNewLogWriterRegionServer extends Thread {
    AtomicBoolean stop;
    public ZombieNewLogWriterRegionServer(AtomicBoolean stop) {
      super("ZombieNewLogWriterRegionServer");
      this.stop = stop;
    }

    @Override
    public void run() {
      if (stop.get()) {
        return;
      }
      Path tableDir = new Path(hbaseDir, new String(TABLE_NAME));
      Path regionDir = new Path(tableDir, regions.get(0));
      Path recoveredEdits = new Path(regionDir, HLogSplitter.RECOVERED_EDITS);
      String region = "juliet";
      Path julietLog = new Path(hlogDir, HLOG_FILE_PREFIX + ".juliet");
      try {

        while (!fs.exists(recoveredEdits) && !stop.get()) {
          flushToConsole("Juliet: split not started, sleeping a bit...");
          Threads.sleep(10);
        }
 
        fs.mkdirs(new Path(tableDir, region));
        HLog.Writer writer = HLog.createWriter(fs,
            julietLog, conf);
        appendEntry(writer, "juliet".getBytes(), ("juliet").getBytes(),
            ("r").getBytes(), FAMILY, QUALIFIER, VALUE, 0);
        writer.close();
        flushToConsole("Juliet file creator: created file " + julietLog);
      } catch (IOException e1) {
        assertTrue("Failed to create file " + julietLog, false);
      }
    }
  }

  private CancelableProgressable reporter = new CancelableProgressable() {
    int count = 0;

    @Override
    public boolean progress() {
      count++;
      LOG.debug("progress = " + count);
      return true;
    }
  };

  @Test
  public void testSplitLogFileWithOneRegion() throws IOException {
    LOG.info("testSplitLogFileWithOneRegion");
    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);


    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];
    fs.initialize(fs.getUri(), conf);
    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs,
        conf, reporter);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);


    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, REGION);


    assertEquals(true, logsAreEqual(originalLog, splitLog));
  }

  @Test
  public void testSplitLogFileEmpty() throws IOException {
    LOG.info("testSplitLogFileEmpty");
    injectEmptyFile(".empty", true);
    FileStatus logfile = fs.listStatus(hlogDir)[0];

    fs.initialize(fs.getUri(), conf);

    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs,
        conf, reporter);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);
    Path tdir = HTableDescriptor.getTableDir(hbaseDir, TABLE_NAME);
    assertFalse(fs.exists(tdir));

    assertEquals(0, countHLog(fs.listStatus(oldLogDir)[0].getPath(), fs, conf));
  }

  @Test
  public void testSplitLogFileMultipleRegions() throws IOException {
    LOG.info("testSplitLogFileMultipleRegions");
    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];
    fs.initialize(fs.getUri(), conf);

    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs,
        conf, reporter);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);
    for (String region : regions) {
      Path recovered = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(10, countHLog(recovered, fs, conf));
    }
  }

  @Test
  public void testSplitLogFileFirstLineCorruptionLog()
  throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];

    corruptHLog(logfile.getPath(),
        Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs,
        conf, reporter);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);

    final Path corruptDir = new Path(conf.get(HConstants.HBASE_DIR), conf.get(
        "hbase.regionserver.hlog.splitlog.corrupt.dir", ".corrupt"));
    assertEquals(1, fs.listStatus(corruptDir).length);
  }


  private void flushToConsole(String s) {
    System.out.println(s);
    System.out.flush();
  }


  private void generateHLogs(int leaveOpen) throws IOException {
    generateHLogs(NUM_WRITERS, ENTRIES, leaveOpen);
  }

  private void makeRegionDirs(FileSystem fs, List<String> regions) throws IOException {
    for (String region : regions) {
      flushToConsole("Creating dir for region " + region);
      fs.mkdirs(new Path(tabledir, region));
    }
  }

  private void generateHLogs(int writers, int entries, int leaveOpen) throws IOException {
    makeRegionDirs(fs, regions);
    for (int i = 0; i < writers; i++) {
      writer[i] = HLog.createWriter(fs, new Path(hlogDir, HLOG_FILE_PREFIX + i), conf);
      for (int j = 0; j < entries; j++) {
        int prefix = 0;
        for (String region : regions) {
          String row_key = region + prefix++ + i + j;
          appendEntry(writer[i], TABLE_NAME, region.getBytes(),
                  row_key.getBytes(), FAMILY, QUALIFIER, VALUE, seq);
        }
      }
      if (i != leaveOpen) {
        writer[i].close();
        flushToConsole("Closing writer " + i);
      }
    }
  }

  private Path getLogForRegion(Path rootdir, byte[] table, String region)
  throws IOException {
    Path tdir = HTableDescriptor.getTableDir(rootdir, table);
    Path editsdir = HLog.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
      Bytes.toString(region.getBytes())));
    FileStatus [] files = this.fs.listStatus(editsdir);
    assertEquals(1, files.length);
    return files[0].getPath();
  }

  private void corruptHLog(Path path, Corruptions corruption, boolean close,
                           FileSystem fs) throws IOException {

    FSDataOutputStream out;
    int fileSize = (int) fs.listStatus(path)[0].getLen();

    FSDataInputStream in = fs.open(path);
    byte[] corrupted_bytes = new byte[fileSize];
    in.readFully(0, corrupted_bytes, 0, fileSize);
    in.close();

    switch (corruption) {
      case APPEND_GARBAGE:
        out = fs.append(path);
        out.write("-----".getBytes());
        closeOrFlush(close, out);
        break;

      case INSERT_GARBAGE_ON_FIRST_LINE:
        fs.delete(path, false);
        out = fs.create(path);
        out.write(0);
        out.write(corrupted_bytes);
        closeOrFlush(close, out);
        break;

      case INSERT_GARBAGE_IN_THE_MIDDLE:
        fs.delete(path, false);
        out = fs.create(path);
        int middle = (int) Math.floor(corrupted_bytes.length / 2);
        out.write(corrupted_bytes, 0, middle);
        out.write(0);
        out.write(corrupted_bytes, middle, corrupted_bytes.length - middle);
        closeOrFlush(close, out);
        break;

      case TRUNCATE:
        fs.delete(path, false);
        out = fs.create(path);
        out.write(corrupted_bytes, 0, fileSize-32);
        closeOrFlush(close, out);

        break;
    }


  }

  private void closeOrFlush(boolean close, FSDataOutputStream out)
  throws IOException {
    if (close) {
      out.close();
    } else {
      out.sync();
      // Not in 0out.hflush();
    }
  }

  @SuppressWarnings("unused")
  private void dumpHLog(Path log, FileSystem fs, Configuration conf) throws IOException {
    HLog.Entry entry;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while ((entry = in.next()) != null) {
      System.out.println(entry);
    }
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf) throws IOException {
    int count = 0;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }


  public long appendEntry(HLog.Writer writer, byte[] table, byte[] region,
                          byte[] row, byte[] family, byte[] qualifier,
                          byte[] value, long seq)
          throws IOException {

    writer.append(createTestEntry(table, region, row, family, qualifier, value, seq));
    writer.sync();
    return seq;
  }

  private HLog.Entry createTestEntry(
      byte[] table, byte[] region,
      byte[] row, byte[] family, byte[] qualifier,
      byte[] value, long seq) {
    long time = System.nanoTime();
    WALEdit edit = new WALEdit();
    seq++;
    edit.add(new KeyValue(row, family, qualifier, time, KeyValue.Type.Put, value));
    return new HLog.Entry(new HLogKey(region, table, seq, time), edit);
  }


  private void injectEmptyFile(String suffix, boolean closeFile)
          throws IOException {
    HLog.Writer writer = HLog.createWriter(
            fs, new Path(hlogDir, HLOG_FILE_PREFIX + suffix), conf);
    if (closeFile) writer.close();
  }

  @SuppressWarnings("unused")
  private void listLogs(FileSystem fs, Path dir) throws IOException {
    for (FileStatus file : fs.listStatus(dir)) {
      System.out.println(file.getPath());
    }

  }

  private int compareHLogSplitDirs(Path p1, Path p2) throws IOException {
    FileStatus[] f1 = fs.listStatus(p1);
    FileStatus[] f2 = fs.listStatus(p2);
    assertNotNull("Path " + p1 + " doesn't exist", f1);
    assertNotNull("Path " + p2 + " doesn't exist", f2);

    System.out.println("Files in " + p1 + ": " +
        Joiner.on(",").join(FileUtil.stat2Paths(f1)));
    System.out.println("Files in " + p2 + ": " +
        Joiner.on(",").join(FileUtil.stat2Paths(f2)));
    assertEquals(f1.length, f2.length);

    for (int i = 0; i < f1.length; i++) {
      // Regions now have a directory named RECOVERED_EDITS_DIR and in here
      // are split edit files. In below presume only 1.
      Path rd1 = HLog.getRegionDirRecoveredEditsDir(f1[i].getPath());
      FileStatus[] rd1fs = fs.listStatus(rd1);
      assertEquals(1, rd1fs.length);
      Path rd2 = HLog.getRegionDirRecoveredEditsDir(f2[i].getPath());
      FileStatus[] rd2fs = fs.listStatus(rd2);
      assertEquals(1, rd2fs.length);
      if (!logsAreEqual(rd1fs[0].getPath(), rd2fs[0].getPath())) {
        return -1;
      }
    }
    return 0;
  }

  private boolean logsAreEqual(Path p1, Path p2) throws IOException {
    HLog.Reader in1, in2;
    in1 = HLog.getReader(fs, p1, conf);
    in2 = HLog.getReader(fs, p2, conf);
    HLog.Entry entry1;
    HLog.Entry entry2;
    while ((entry1 = in1.next()) != null) {
      entry2 = in2.next();
      if ((entry1.getKey().compareTo(entry2.getKey()) != 0) ||
              (!entry1.getEdit().toString().equals(entry2.getEdit().toString()))) {
        return false;
      }
    }
    return true;
  }
}
