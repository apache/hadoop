/*
 * Copyright 2009 The Apache Software Foundation
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

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.security.UnixUserGroupInformation;

import com.google.common.base.Joiner;

/**
 * Test class fosr the Store
 */
public class TestStore extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestStore.class);

  Store store;
  byte [] table = Bytes.toBytes("table");
  byte [] family = Bytes.toBytes("family");

  byte [] row = Bytes.toBytes("row");
  byte [] qf1 = Bytes.toBytes("qf1");
  byte [] qf2 = Bytes.toBytes("qf2");
  byte [] qf3 = Bytes.toBytes("qf3");
  byte [] qf4 = Bytes.toBytes("qf4");
  byte [] qf5 = Bytes.toBytes("qf5");
  byte [] qf6 = Bytes.toBytes("qf6");

  NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

  List<KeyValue> expected = new ArrayList<KeyValue>();
  List<KeyValue> result = new ArrayList<KeyValue>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private static final String DIR = HBaseTestingUtility.getTestDir() + "/TestStore/";

  /**
   * Setup
   * @throws IOException
   */
  @Override
  public void setUp() throws IOException {
    qualifiers.add(qf1);
    qualifiers.add(qf3);
    qualifiers.add(qf5);

    Iterator<byte[]> iter = qualifiers.iterator();
    while(iter.hasNext()){
      byte [] next = iter.next();
      expected.add(new KeyValue(row, family, next, 1, (byte[])null));
      get.addColumn(family, next);
    }
  }

  private void init(String methodName) throws IOException {
    init(methodName, HBaseConfiguration.create());
  }

  private void init(String methodName, Configuration conf)
  throws IOException {
    //Setting up a Store
    Path basedir = new Path(DIR+methodName);
    Path logdir = new Path(DIR+methodName+"/logs");
    Path oldLogDir = new Path(basedir, HConstants.HREGION_OLDLOGDIR_NAME);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    HLog hlog = new HLog(fs, logdir, oldLogDir, conf, null);
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, null);

    store = new Store(basedir, region, hcd, fs, conf);
  }


  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Test for hbase-1686.
   * @throws IOException
   */
  public void testEmptyStoreFile() throws IOException {
    init(this.getName());
    // Write a store file.
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    flush(1);
    // Now put in place an empty store file.  Its a little tricky.  Have to
    // do manually with hacked in sequence id.
    StoreFile f = this.store.getStorefiles().get(0);
    Path storedir = f.getPath().getParent();
    long seqid = f.getMaxSequenceId();
    Configuration c = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(c);
    StoreFile.Writer w = StoreFile.createWriter(fs, storedir,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL);
    w.appendMetadata(seqid + 1, false);
    w.close();
    this.store.close();
    // Reopen it... should pick up two files
    this.store = new Store(storedir.getParent().getParent(),
      this.store.getHRegion(),
      this.store.getFamily(), fs, c);
    System.out.println(this.store.getHRegionInfo().getEncodedName());
    assertEquals(2, this.store.getStorefilesCount());

    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(),
        qualifiers);
    assertEquals(1, result.size());
  }

  /**
   * Getting data from memstore only
   * @throws IOException
   */
  public void testGet_FromMemStoreOnly() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null));

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(), qualifiers);

    //Compare
    assertCheck();
  }

  /**
   * Getting data from files only
   * @throws IOException
   */
  public void testGet_FromFilesOnly() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null));
    //flush
    flush(3);

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(),
        qualifiers);
    //this.store.get(get, qualifiers, result);

    //Need to sort the result since multiple files
    Collections.sort(result, KeyValue.COMPARATOR);

    //Compare
    assertCheck();
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  public void testGet_FromMemStoreAndFiles() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null));

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(), qualifiers);

    //Need to sort the result since multiple files
    Collections.sort(result, KeyValue.COMPARATOR);

    //Compare
    assertCheck();
  }

  private void flush(int storeFilessize) throws IOException{
    this.store.snapshot();
    flushStore(store, id++);
    assertEquals(storeFilessize, this.store.getStorefiles().size());
    assertEquals(0, this.store.memstore.kvset.size());
  }

  private void assertCheck() {
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // IncrementColumnValue tests
  //////////////////////////////////////////////////////////////////////////////
  /*
   * test the internal details of how ICV works, especially during a flush scenario.
   */
  public void testIncrementColumnValue_ICVDuringFlush()
      throws IOException, InterruptedException {
    init(this.getName());

    long oldValue = 1L;
    long newValue = 3L;
    this.store.add(new KeyValue(row, family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(oldValue)));

    // snapshot the store.
    this.store.snapshot();

    // add other things:
    this.store.add(new KeyValue(row, family, qf2,
        System.currentTimeMillis(),
        Bytes.toBytes(oldValue)));

    // update during the snapshot.
    long ret = this.store.updateColumnValue(row, family, qf1, newValue);

    // memstore should have grown by some amount.
    assertTrue(ret > 0);

    // then flush.
    flushStore(store, id++);
    assertEquals(1, this.store.getStorefiles().size());
    // from the one we inserted up there, and a new one
    assertEquals(2, this.store.memstore.kvset.size());

    // how many key/values for this row are there?
    Get get = new Get(row);
    get.addColumn(family, qf1);
    get.setMaxVersions(); // all versions.
    List<KeyValue> results = new ArrayList<KeyValue>();

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    assertEquals(2, results.size());

    long ts1 = results.get(0).getTimestamp();
    long ts2 = results.get(1).getTimestamp();

    assertTrue(ts1 > ts2);

    assertEquals(newValue, Bytes.toLong(results.get(0).getValue()));
    assertEquals(oldValue, Bytes.toLong(results.get(1).getValue()));
  }

  public void testIncrementColumnValue_SnapshotFlushCombo() throws Exception {
    ManualEnvironmentEdge mee = new ManualEnvironmentEdge();
    EnvironmentEdgeManagerTestHelper.injectEdge(mee);
    init(this.getName());

    long oldValue = 1L;
    long newValue = 3L;
    this.store.add(new KeyValue(row, family, qf1,
        EnvironmentEdgeManager.currentTimeMillis(),
        Bytes.toBytes(oldValue)));

    // snapshot the store.
    this.store.snapshot();

    // update during the snapshot, the exact same TS as the Put (lololol)
    long ret = this.store.updateColumnValue(row, family, qf1, newValue);

    // memstore should have grown by some amount.
    assertTrue(ret > 0);

    // then flush.
    flushStore(store, id++);
    assertEquals(1, this.store.getStorefiles().size());
    assertEquals(1, this.store.memstore.kvset.size());

    // now increment again:
    newValue += 1;
    this.store.updateColumnValue(row, family, qf1, newValue);

    // at this point we have a TS=1 in snapshot, and a TS=2 in kvset, so increment again:
    newValue += 1;
    this.store.updateColumnValue(row, family, qf1, newValue);

    // the second TS should be TS=2 or higher., even though 'time=1' right now.


    // how many key/values for this row are there?
    Get get = new Get(row);
    get.addColumn(family, qf1);
    get.setMaxVersions(); // all versions.
    List<KeyValue> results = new ArrayList<KeyValue>();

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    assertEquals(2, results.size());

    long ts1 = results.get(0).getTimestamp();
    long ts2 = results.get(1).getTimestamp();

    assertTrue(ts1 > ts2);
    assertEquals(newValue, Bytes.toLong(results.get(0).getValue()));
    assertEquals(oldValue, Bytes.toLong(results.get(1).getValue()));

    mee.setValue(2); // time goes up slightly
    newValue += 1;
    this.store.updateColumnValue(row, family, qf1, newValue);

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    assertEquals(2, results.size());

    ts1 = results.get(0).getTimestamp();
    ts2 = results.get(1).getTimestamp();

    assertTrue(ts1 > ts2);
    assertEquals(newValue, Bytes.toLong(results.get(0).getValue()));
    assertEquals(oldValue, Bytes.toLong(results.get(1).getValue()));
  }

  public void testHandleErrorsInFlush() throws Exception {
    LOG.info("Setting up a faulty file system that cannot write");

    Configuration conf = HBaseConfiguration.create();
    // Set a different UGI so we don't get the same cached LocalFS instance
    conf.set(UnixUserGroupInformation.UGI_PROPERTY_NAME,
        "testhandleerrorsinflush,foo");
    // Inject our faulty LocalFileSystem
    conf.setClass("fs.file.impl", FaultyFileSystem.class,
        FileSystem.class);
    // Make sure it worked (above is sensitive to caching details in hadoop core)
    FileSystem fs = FileSystem.get(conf);
    assertEquals(FaultyFileSystem.class, fs.getClass());

    // Initialize region
    init(getName(), conf);

    LOG.info("Adding some data");
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));

    LOG.info("Before flush, we should have no files");
    FileStatus[] files = fs.listStatus(store.getHomedir());
    Path[] paths = FileUtil.stat2Paths(files);
    System.err.println("Got paths: " + Joiner.on(",").join(paths));
    assertEquals(0, paths.length);

    //flush
    try {
      LOG.info("Flushing");
      flush(1);
      fail("Didn't bubble up IOE!");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("Fault injected"));
    }

    LOG.info("After failed flush, we should still have no files!");
    files = fs.listStatus(store.getHomedir());
    paths = FileUtil.stat2Paths(files);
    System.err.println("Got paths: " + Joiner.on(",").join(paths));
    assertEquals(0, paths.length);
  }


  static class FaultyFileSystem extends FilterFileSystem {
    List<SoftReference<FaultyOutputStream>> outStreams =
      new ArrayList<SoftReference<FaultyOutputStream>>();
    private long faultPos = 200;

    public FaultyFileSystem() {
      super(new LocalFileSystem());
      System.err.println("Creating faulty!");
    }

    @Override
    public FSDataOutputStream create(Path p) throws IOException {
      return new FaultyOutputStream(super.create(p), faultPos);
    }

  }

  static class FaultyOutputStream extends FSDataOutputStream {
    volatile long faultPos = Long.MAX_VALUE;

    public FaultyOutputStream(FSDataOutputStream out,
        long faultPos) throws IOException {
      super(out, null);
      this.faultPos = faultPos;
    }

    @Override
    public void write(byte[] buf, int offset, int length) throws IOException {
      System.err.println("faulty stream write at pos " + getPos());
      injectFault();
      super.write(buf, offset, length);
    }

    private void injectFault() throws IOException {
      if (getPos() >= faultPos) {
        throw new IOException("Fault injected");
      }
    }
  }



  private static void flushStore(Store store, long id) throws IOException {
    StoreFlusher storeFlusher = store.getStoreFlusher(id);
    storeFlusher.prepare();
    storeFlusher.flushCache();
    storeFlusher.commit();
  }



  /**
   * Generate a list of KeyValues for testing based on given parameters
   * @param timestamps
   * @param numRows
   * @param qualifier
   * @param family
   * @return
   */
  List<KeyValue> getKeyValueSet(long[] timestamps, int numRows,
      byte[] qualifier, byte[] family) {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    for (int i=1;i<=numRows;i++) {
      byte[] b = Bytes.toBytes(i);
      for (long timestamp: timestamps) {
        kvList.add(new KeyValue(b, family, qualifier, timestamp, b));
      }
    }
    return kvList;
  }

  /**
   * Test to ensure correctness when using Stores with multiple timestamps
   * @throws IOException
   */
  public void testMultipleTimestamps() throws IOException {
    int numRows = 1;
    long[] timestamps1 = new long[] {1,5,10,20};
    long[] timestamps2 = new long[] {30,80};

    init(this.getName());

    List<KeyValue> kvList1 = getKeyValueSet(timestamps1,numRows, qf1, family);
    for (KeyValue kv : kvList1) {
      this.store.add(kv);
    }

    this.store.snapshot();
    flushStore(store, id++);

    List<KeyValue> kvList2 = getKeyValueSet(timestamps2,numRows, qf1, family);
    for(KeyValue kv : kvList2) {
      this.store.add(kv);
    }

    List<KeyValue> result;
    Get get = new Get(Bytes.toBytes(1));
    get.addColumn(family,qf1);

    get.setTimeRange(0,15);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(40,90);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(10,45);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(80,145);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(1,2);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(90,200);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()==0);
  }
}
