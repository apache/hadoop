/**
 * Copyright 2010 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testing {@link HLog} splitting code.
 */
public class TestHLogSplit {

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


  static enum Corruptions {
    INSERT_GARBAGE_ON_FIRST_LINE,
    INSERT_GARBAGE_IN_THE_MIDDLE,
    APPEND_GARBAGE,
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().
            setInt("hbase.regionserver.flushlogentries", 1);
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
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries){
      fs.delete(dir.getPath(), true);
    }
    seq = 0;
    regions = new ArrayList<String>();
    Collections.addAll(regions, "bbb", "ccc");
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    // Set the soft lease for hdfs to be down from default of 5 minutes or so.
    // TODO: If 0.20 hadoop do one thing, if 0.21 hadoop do another.
    // Not available in 0.20 hdfs
    // TEST_UTIL.getDFSCluster().getNamesystem().leaseManager.
    //  setLeasePeriod(100, 50000);
    // Use reflection to get at the 0.20 version of above.
    MiniDFSCluster dfsCluster = TEST_UTIL.getDFSCluster();
    //   private NameNode nameNode;
    Field field = dfsCluster.getClass().getDeclaredField("nameNode");
    field.setAccessible(true);
    NameNode nn = (NameNode)field.get(dfsCluster);
    nn.namesystem.leaseManager.setLeasePeriod(100, 50000);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test(expected = IOException.class)
  public void testSplitFailsIfNewHLogGetsCreatedAfterSplitStarted()
  throws IOException {
    AtomicBoolean stop = new AtomicBoolean(false);
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    try {
    (new ZombieNewLogWriterRegionServer(stop)).start();
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
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
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

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

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);


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

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

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

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

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
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

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
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

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
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

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

    Path c1 = new Path(hlogDir, HLOG_FILE_PREFIX + "0");
    Path c2 = new Path(hlogDir, HLOG_FILE_PREFIX + "5");
    Path c3 = new Path(hlogDir, HLOG_FILE_PREFIX + (NUM_WRITERS - 1));
    generateHLogs(-1);
    corruptHLog(c1, Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false, fs);
    corruptHLog(c2, Corruptions.APPEND_GARBAGE, true, fs);
    corruptHLog(c3, Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    FileStatus[] archivedLogs = fs.listStatus(corruptDir);

    assertEquals("expected a different file", c1.getName(), archivedLogs[0].getPath().getName());
    assertEquals("expected a different file", c2.getName(), archivedLogs[1].getPath().getName());
    assertEquals("expected a different file", c3.getName(), archivedLogs[2].getPath().getName());
    assertEquals(archivedLogs.length, 3);

  }

  @Test
  public void testLogsGetArchivedAfterSplit() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    FileStatus[] archivedLogs = fs.listStatus(oldLogDir);

    assertEquals("wrong number of files in the archive log", NUM_WRITERS, archivedLogs.length);
  }



  @Test(expected = IOException.class)
  public void testTrailingGarbageCorruptionLogFileSkipErrorsFalseThrows() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
  }

  @Test
  public void testCorruptedLogFilesSkipErrorsFalseDoesNotTouchLogs() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateHLogs(-1);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);
    fs.initialize(fs.getUri(), conf);
    try {
      HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    } catch (IOException e) {/* expected */}

    assertEquals("if skip.errors is false all files should remain in place",
            NUM_WRITERS, fs.listStatus(hlogDir).length);
  }


  @Test
  public void testSplit() throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);



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
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    FileStatus [] statuses = null;
    try {
      statuses = fs.listStatus(hlogDir);
      assertNull(statuses);
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
        HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
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

    InstrumentedSequenceFileLogWriter.activateFailure = false;
    appendEntry(writer[4], TABLE_NAME, Bytes.toBytes("break"), ("r" + 999).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
    writer[4].close();


    try {
      InstrumentedSequenceFileLogWriter.activateFailure = true;
      HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    } catch (IOException e) {
      assertEquals("java.io.IOException: This exception is instrumented and should only be thrown for testing", e.getMessage());
      throw e;
    } finally {
      InstrumentedSequenceFileLogWriter.activateFailure = false;
    }
  }


//  @Test
  public void testSplittingLargeNumberOfRegionsConsistency() throws IOException {

    regions.removeAll(regions);
    for (int i=0; i<100; i++) {
      regions.add("region__"+i);
    }

    generateHLogs(1, 100, -1);
    fs.initialize(fs.getUri(), conf);

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    fs.rename(oldLogDir, hlogDir);
    Path firstSplitPath = new Path(hbaseDir, Bytes.toString(TABLE_NAME) + ".first");
    Path splitPath = new Path(hbaseDir, Bytes.toString(TABLE_NAME));
    fs.rename(splitPath,
            firstSplitPath);


    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    assertEquals(0, compareHLogSplitDirs(firstSplitPath, splitPath));
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

          appendEntry(lastLogWriter, TABLE_NAME, "juliet".getBytes(),
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
      boolean splitStarted = false;
      Path p = new Path(hbaseDir, new String(TABLE_NAME));
      while (!splitStarted) {
        try {
          FileStatus [] statuses = fs.listStatus(p);
          // In 0.20, listStatus comes back with a null if file doesn't exit.
          // In 0.21, it throws FNFE.
          if (statuses != null && statuses.length > 0) {
            // Done.
            break;
          }
        } catch (FileNotFoundException e) {
          // Expected in hadoop 0.21
        } catch (IOException e1) {
          assertTrue("Failed to list status ", false);
        }
        flushToConsole("Juliet: split not started, sleeping a bit...");
        Threads.sleep(100);
      }

      Path julietLog = new Path(hlogDir, HLOG_FILE_PREFIX + ".juliet");
      try {
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

  private void flushToConsole(String s) {
    System.out.println(s);
    System.out.flush();
  }


  private void generateHLogs(int leaveOpen) throws IOException {
    generateHLogs(NUM_WRITERS, ENTRIES, leaveOpen);
  }

  private void generateHLogs(int writers, int entries, int leaveOpen) throws IOException {
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

  private Path getLogForRegion(Path rootdir, byte[] table, String region) {
    return new Path(HRegion.getRegionDir(HTableDescriptor
            .getTableDir(rootdir, table),
            HRegionInfo.encodeRegionName(region.getBytes())),
            HConstants.HREGION_OLDLOGFILE_NAME);
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

    long time = System.nanoTime();
    WALEdit edit = new WALEdit();
    seq++;
    edit.add(new KeyValue(row, family, qualifier, time, KeyValue.Type.Put, value));
    writer.append(new HLog.Entry(new HLogKey(region, table, seq, time), edit));
    writer.sync();
    return seq;

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

    for (int i=0; i<f1.length; i++) {
      if (!logsAreEqual(new Path(f1[i].getPath(), HConstants.HREGION_OLDLOGFILE_NAME),
              new Path(f2[i].getPath(), HConstants.HREGION_OLDLOGFILE_NAME))) {
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
