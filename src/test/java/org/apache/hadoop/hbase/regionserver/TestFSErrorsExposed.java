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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


/**
 * Test cases that ensure that file system level errors are bubbled up
 * appropriately to clients, rather than swallowed.
 */
public class TestFSErrorsExposed {
  private static final Log LOG = LogFactory.getLog(TestFSErrorsExposed.class);
  
  HBaseTestingUtility util = new HBaseTestingUtility();
  
  /**
   * Injects errors into the pread calls of an on-disk file, and makes
   * sure those bubble up to the HFile scanner
   */
  @Test
  public void testHFileScannerThrowsErrors() throws IOException {
    Path hfilePath = new Path(new Path(
        HBaseTestingUtility.getTestDir("internalScannerExposesErrors"),
        "regionname"), "familyname");
    FaultyFileSystem fs = new FaultyFileSystem(util.getTestFileSystem());
    StoreFile.Writer writer = StoreFile.createWriter(fs, hfilePath, 2*1024);
    TestStoreFile.writeStoreFile(
        writer, Bytes.toBytes("cf"), Bytes.toBytes("qual"));
    
    StoreFile sf = new StoreFile(fs, writer.getPath(), false,
        util.getConfiguration(), StoreFile.BloomType.NONE, false);
    HFile.Reader reader = sf.createReader();
    HFileScanner scanner = reader.getScanner(false, true);
    
    FaultyInputStream inStream = fs.inStreams.get(0).get();
    assertNotNull(inStream);
    
    scanner.seekTo();
    // Do at least one successful read
    assertTrue(scanner.next());
    
    inStream.startFaults();
    
    try {
      int scanned=0;
      while (scanner.next()) {
        scanned++;
      }
      fail("Scanner didn't throw after faults injected");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      assertTrue(ioe.getMessage().contains("Fault"));
    }
    reader.close();
  }
  
  /**
   * Injects errors into the pread calls of an on-disk file, and makes
   * sure those bubble up to the StoreFileScanner
   */
  @Test
  public void testStoreFileScannerThrowsErrors() throws IOException {
    Path hfilePath = new Path(new Path(
        HBaseTestingUtility.getTestDir("internalScannerExposesErrors"),
        "regionname"), "familyname");
    FaultyFileSystem fs = new FaultyFileSystem(util.getTestFileSystem());
    HFile.Writer writer = StoreFile.createWriter(fs, hfilePath, 2 * 1024);
    TestStoreFile.writeStoreFile(
        writer, Bytes.toBytes("cf"), Bytes.toBytes("qual"));
    
    StoreFile sf = new StoreFile(fs, writer.getPath(), false,
        util.getConfiguration(), BloomType.NONE, false);
    List<StoreFileScanner> scanners = StoreFileScanner.getScannersForStoreFiles(
        Collections.singletonList(sf), false, true);
    KeyValueScanner scanner = scanners.get(0);
    
    FaultyInputStream inStream = fs.inStreams.get(0).get();
    assertNotNull(inStream);
    
    scanner.seek(KeyValue.LOWESTKEY);
    // Do at least one successful read
    assertNotNull(scanner.next());
    
    inStream.startFaults();
    
    try {
      int scanned=0;
      while (scanner.next() != null) {
        scanned++;
      }
      fail("Scanner didn't throw after faults injected");
    } catch (IOException ioe) {
      LOG.info("Got expected exception", ioe);
      assertTrue(ioe.getMessage().contains("Could not iterate"));
    }
    scanner.close();
  }
  
  /**
   * Cluster test which starts a region server with a region, then
   * removes the data from HDFS underneath it, and ensures that
   * errors are bubbled to the client.
   */
  @Test
  public void testFullSystemBubblesFSErrors() throws Exception {
    try {
      util.startMiniCluster(1);
      byte[] tableName = Bytes.toBytes("table");
      byte[] fam = Bytes.toBytes("fam");
      
      HBaseAdmin admin = new HBaseAdmin(util.getConfiguration());
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(
          fam, 1, HColumnDescriptor.DEFAULT_COMPRESSION,
          false, false, HConstants.FOREVER, "NONE"));
      admin.createTable(desc);
      
      HTable table = new HTable(tableName);
      
      // Load some data
      util.loadTable(table, fam);
      table.flushCommits();
      util.flush();
      util.countRows(table);
            
      // Kill the DFS cluster
      util.getDFSCluster().shutdownDataNodes();
      
      try {
        util.countRows(table);
        fail("Did not fail to count after removing data");
      } catch (RuntimeException rte) {
        // We get RTE instead of IOE since java Iterable<?> doesn't throw
        // IOE
        LOG.info("Got expected error", rte);
        assertTrue(rte.getMessage().contains("Could not seek"));
      }
      
    } finally {
      util.shutdownMiniCluster();
    }
  }
  
  static class FaultyFileSystem extends FilterFileSystem {
    List<SoftReference<FaultyInputStream>> inStreams =
      new ArrayList<SoftReference<FaultyInputStream>>();
    
    public FaultyFileSystem(FileSystem testFileSystem) {
      super(testFileSystem);
    }

    @Override
    public FSDataInputStream open(Path p, int bufferSize) throws IOException  {
      FSDataInputStream orig = fs.open(p, bufferSize);
      FaultyInputStream faulty = new FaultyInputStream(orig);
      inStreams.add(new SoftReference<FaultyInputStream>(faulty));
      return faulty;
    }
  }
  
  static class FaultyInputStream extends FSDataInputStream {
    boolean faultsStarted = false;
    
    public FaultyInputStream(InputStream in) throws IOException {
      super(in);
    }

    public void startFaults() {
      faultsStarted = true;      
    }

    public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
      injectFault();
      return ((PositionedReadable)in).read(position, buffer, offset, length);
    }
    
    private void injectFault() throws IOException {
      if (faultsStarted) {
        throw new IOException("Fault injected");
      }
    }
  }

  
}
