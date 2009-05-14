/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.util.Bytes;

/** JUnit test case for HLog */
public class DisabledTestTransactionalHLogManager extends HBaseTestCase implements
    HConstants {
  private Path dir;
  private MiniDFSCluster cluster;

  final byte[] tableName = Bytes.toBytes("tablename");
  final HTableDescriptor tableDesc = new HTableDescriptor(tableName);
  final HRegionInfo regionInfo = new HRegionInfo(tableDesc,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
  final byte[] row1 = Bytes.toBytes("row1");
  final byte[] val1 = Bytes.toBytes("val1");
  final byte[] row2 = Bytes.toBytes("row2");
  final byte[] val2 = Bytes.toBytes("val2");
  final byte[] row3 = Bytes.toBytes("row3");
  final byte[] val3 = Bytes.toBytes("val3");
  final byte[] col = Bytes.toBytes("col:A");

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster(conf, 2, true, (String[]) null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    this.conf.set(HConstants.HBASE_DIR, this.cluster.getFileSystem()
        .getHomeDirectory().toString());
    super.setUp();
    this.dir = new Path("/hbase", getName());
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }
  }

  @Override
  public void tearDown() throws Exception {
    if (this.fs.exists(this.dir)) {
      this.fs.delete(this.dir, true);
    }
    shutdownDfs(cluster);
    super.tearDown();
  }

  /**
   * @throws IOException
   */
  public void testSingleCommit() throws IOException {

    HLog log = new HLog(fs, dir, this.conf, null);
    TransactionalHLogManager logMangaer = new TransactionalHLogManager(log, fs,
        regionInfo, conf);

    // Write columns named 1, 2, 3, etc. and then values of single byte
    // 1, 2, 3...
    long transactionId = 1;
    logMangaer.writeStartToLog(transactionId);

    BatchUpdate update1 = new BatchUpdate(row1);
    update1.put(col, val1);
    logMangaer.writeUpdateToLog(transactionId, update1);

    BatchUpdate update2 = new BatchUpdate(row2);
    update2.put(col, val2);
    logMangaer.writeUpdateToLog(transactionId, update2);

    BatchUpdate update3 = new BatchUpdate(row3);
    update3.put(col, val3);
    logMangaer.writeUpdateToLog(transactionId, update3);

    logMangaer.writeCommitToLog(transactionId);

    // log.completeCacheFlush(regionName, tableName, logSeqId);

    log.close();
    Path filename = log.computeFilename(log.getFilenum());

    Map<Long, List<BatchUpdate>> commits = logMangaer.getCommitsFromLog(
        filename, -1, null);

    assertEquals(1, commits.size());
    assertTrue(commits.containsKey(transactionId));
    assertEquals(3, commits.get(transactionId).size());

    List<BatchUpdate> updates = commits.get(transactionId);

    update1 = updates.get(0);
    assertTrue(Bytes.equals(row1, update1.getRow()));
    assertTrue(Bytes.equals(val1, update1.iterator().next().getValue()));

    update2 = updates.get(1);
    assertTrue(Bytes.equals(row2, update2.getRow()));
    assertTrue(Bytes.equals(val2, update2.iterator().next().getValue()));

    update3 = updates.get(2);
    assertTrue(Bytes.equals(row3, update3.getRow()));
    assertTrue(Bytes.equals(val3, update3.iterator().next().getValue()));

  }
  
  /**
   * @throws IOException
   */
  public void testSingleAbort() throws IOException {

    HLog log = new HLog(fs, dir, this.conf, null);
    TransactionalHLogManager logMangaer = new TransactionalHLogManager(log, fs,
        regionInfo, conf);

    long transactionId = 1;
    logMangaer.writeStartToLog(transactionId);

    BatchUpdate update1 = new BatchUpdate(row1);
    update1.put(col, val1);
    logMangaer.writeUpdateToLog(transactionId, update1);

    BatchUpdate update2 = new BatchUpdate(row2);
    update2.put(col, val2);
    logMangaer.writeUpdateToLog(transactionId, update2);

    BatchUpdate update3 = new BatchUpdate(row3);
    update3.put(col, val3);
    logMangaer.writeUpdateToLog(transactionId, update3);

    logMangaer.writeAbortToLog(transactionId);

    // log.completeCacheFlush(regionName, tableName, logSeqId);

    log.close();
    Path filename = log.computeFilename(log.getFilenum());

    Map<Long, List<BatchUpdate>> commits = logMangaer.getCommitsFromLog(
        filename, -1, null);

    assertEquals(0, commits.size());
  }
  
  /**
   * @throws IOException
   */
  public void testInterlievedCommits() throws IOException {

    HLog log = new HLog(fs, dir, this.conf, null);
    TransactionalHLogManager logMangaer = new TransactionalHLogManager(log, fs,
        regionInfo, conf);

    long transaction1Id = 1;
    long transaction2Id = 2;
    logMangaer.writeStartToLog(transaction1Id);

    BatchUpdate update1 = new BatchUpdate(row1);
    update1.put(col, val1);
    logMangaer.writeUpdateToLog(transaction1Id, update1);

    logMangaer.writeStartToLog(transaction2Id);
    
    BatchUpdate update2 = new BatchUpdate(row2);
    update2.put(col, val2);
    logMangaer.writeUpdateToLog(transaction2Id, update2);

    BatchUpdate update3 = new BatchUpdate(row3);
    update3.put(col, val3);
    logMangaer.writeUpdateToLog(transaction1Id, update3);

    logMangaer.writeCommitToLog(transaction2Id);
    logMangaer.writeCommitToLog(transaction1Id);

    // log.completeCacheFlush(regionName, tableName, logSeqId);

    log.close();
    Path filename = log.computeFilename(log.getFilenum());

    Map<Long, List<BatchUpdate>> commits = logMangaer.getCommitsFromLog(
        filename, -1, null);

    assertEquals(2, commits.size());
    assertEquals(2, commits.get(transaction1Id).size());
    assertEquals(1, commits.get(transaction2Id).size());
  }
  
  /**
   * @throws IOException
   */
  public void testInterlievedAbortCommit() throws IOException {

    HLog log = new HLog(fs, dir, this.conf, null);
    TransactionalHLogManager logMangaer = new TransactionalHLogManager(log, fs,
        regionInfo, conf);

    long transaction1Id = 1;
    long transaction2Id = 2;
    logMangaer.writeStartToLog(transaction1Id);

    BatchUpdate update1 = new BatchUpdate(row1);
    update1.put(col, val1);
    logMangaer.writeUpdateToLog(transaction1Id, update1);

    logMangaer.writeStartToLog(transaction2Id);
    
    BatchUpdate update2 = new BatchUpdate(row2);
    update2.put(col, val2);
    logMangaer.writeUpdateToLog(transaction2Id, update2);

    logMangaer.writeAbortToLog(transaction2Id);
    
    BatchUpdate update3 = new BatchUpdate(row3);
    update3.put(col, val3);
    logMangaer.writeUpdateToLog(transaction1Id, update3);

    logMangaer.writeCommitToLog(transaction1Id);

    // log.completeCacheFlush(regionName, tableName, logSeqId);

    log.close();
    Path filename = log.computeFilename(log.getFilenum());

    Map<Long, List<BatchUpdate>> commits = logMangaer.getCommitsFromLog(
        filename, -1, null);

    assertEquals(1, commits.size());
    assertEquals(2, commits.get(transaction1Id).size());
  }
  
  /**
   * @throws IOException
   */
  public void testInterlievedCommitAbort() throws IOException {

    HLog log = new HLog(fs, dir, this.conf, null);
    TransactionalHLogManager logMangaer = new TransactionalHLogManager(log, fs,
        regionInfo, conf);

    long transaction1Id = 1;
    long transaction2Id = 2;
    logMangaer.writeStartToLog(transaction1Id);

    BatchUpdate update1 = new BatchUpdate(row1);
    update1.put(col, val1);
    logMangaer.writeUpdateToLog(transaction1Id, update1);

    logMangaer.writeStartToLog(transaction2Id);
    
    BatchUpdate update2 = new BatchUpdate(row2);
    update2.put(col, val2);
    logMangaer.writeUpdateToLog(transaction2Id, update2);

    logMangaer.writeCommitToLog(transaction2Id);
    
    BatchUpdate update3 = new BatchUpdate(row3);
    update3.put(col, val3);
    logMangaer.writeUpdateToLog(transaction1Id, update3);

    logMangaer.writeAbortToLog(transaction1Id);

    // log.completeCacheFlush(regionName, tableName, logSeqId);

    log.close();
    Path filename = log.computeFilename(log.getFilenum());

    Map<Long, List<BatchUpdate>> commits = logMangaer.getCommitsFromLog(
        filename, -1, null);

    assertEquals(1, commits.size());
    assertEquals(1, commits.get(transaction2Id).size());
  }
  
  // FIXME Cannot do this test without a global transacton manager
  // public void testMissingCommit() {
  // fail();
  // }

  // FIXME Cannot do this test without a global transacton manager
  // public void testMissingAbort() {
  // fail();
  // }

}
