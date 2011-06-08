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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCatalogJanitor {

  /**
   * Pseudo server for below tests.
   */
  class MockServer implements Server {
    private final Configuration c;
    private final CatalogTracker ct;

    MockServer(final HBaseTestingUtility htu)
    throws NotAllMetaRegionsOnlineException, IOException {
      this.c = htu.getConfiguration();
      // Set hbase.rootdir into test dir.
      FileSystem fs = FileSystem.get(this.c);
      Path rootdir =
        fs.makeQualified(HBaseTestingUtility.getTestDir(HConstants.HBASE_DIR));
      this.c.set(HConstants.HBASE_DIR, rootdir.toString());
      this.ct = Mockito.mock(CatalogTracker.class);
      HRegionInterface hri = Mockito.mock(HRegionInterface.class);
      Mockito.when(ct.waitForMetaServerConnectionDefault()).thenReturn(hri);
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return this.ct;
    }

    @Override
    public Configuration getConfiguration() {
      return this.c;
    }

    @Override
    public ServerName getServerName() {
      return new ServerName("mockserver.example.org", 1234, -1L);
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public void abort(String why, Throwable e) {
      //no-op
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public void stop(String why) {
      //no-op
    }
    
  }

  /**
   * Mock MasterServices for tests below.
   */
  class MockMasterServices implements MasterServices {
    private final MasterFileSystem mfs;
    private final AssignmentManager asm;

    MockMasterServices(final Server server) throws IOException {
      this.mfs = new MasterFileSystem(server, null);
      HTableDescriptor htd = new HTableDescriptor("table");
      htd.addFamily(new HColumnDescriptor("family"));
      this.asm = Mockito.mock(AssignmentManager.class);
      Mockito.when(asm.getTableDescriptor("table")).thenReturn(htd);
    }

    @Override
    public void checkTableModifiable(byte[] tableName) throws IOException {
      //no-op
    }

    @Override
    public AssignmentManager getAssignmentManager() {
      return this.asm;
    }

    @Override
    public ExecutorService getExecutorService() {
      return null;
    }

    @Override
    public MasterFileSystem getMasterFileSystem() {
      return this.mfs;
    }

    @Override
    public ServerManager getServerManager() {
      return null;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public void abort(String why, Throwable e) {
      //no-op
    }

    @Override
    public void stop(String why) {
      //no-op
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }

  @Test
  public void testGetHRegionInfo() throws IOException {
    assertNull(CatalogJanitor.getHRegionInfo(new Result()));
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    Result r = new Result(kvs);
    assertNull(CatalogJanitor.getHRegionInfo(r));
    byte [] f = HConstants.CATALOG_FAMILY;
    // Make a key value that doesn't have the expected qualifier.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.SERVER_QUALIFIER, f));
    r = new Result(kvs);
    assertNull(CatalogJanitor.getHRegionInfo(r));
    // Make a key that does not have a regioninfo value.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.REGIONINFO_QUALIFIER, f));
    HRegionInfo hri = CatalogJanitor.getHRegionInfo(new Result(kvs));
    assertTrue(hri == null);
    // OK, give it what it expects
    kvs.clear();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(HRegionInfo.FIRST_META_REGIONINFO)));
    hri = CatalogJanitor.getHRegionInfo(new Result(kvs));
    assertNotNull(hri);
    assertTrue(hri.equals(HRegionInfo.FIRST_META_REGIONINFO));
  }

  @Test
  public void testCleanParent() throws IOException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);
    CatalogJanitor janitor = new CatalogJanitor(server, services);
    // Create regions.
    HTableDescriptor htd = new HTableDescriptor("table");
    htd.addFamily(new HColumnDescriptor("family"));
    HRegionInfo parent =
      new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
          Bytes.toBytes("eee"));
    HRegionInfo splita =
      new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
          Bytes.toBytes("ccc"));
    HRegionInfo splitb =
      new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
          Bytes.toBytes("eee"));
    // Test that when both daughter regions are in place, that we do not
    // remove the parent.
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
      HConstants.SPLITA_QUALIFIER, Writables.getBytes(splita)));
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
      HConstants.SPLITB_QUALIFIER, Writables.getBytes(splitb)));
    Result r = new Result(kvs);
    // Add a reference under splitA directory so we don't clear out the parent.
    Path rootdir = services.getMasterFileSystem().getRootDir();
    Path tabledir =
      HTableDescriptor.getTableDir(rootdir, htd.getName());
    Path storedir = Store.getStoreHomedir(tabledir, splita.getEncodedName(),
      htd.getColumnFamilies()[0].getName());
    Reference ref = new Reference(Bytes.toBytes("ccc"), Reference.Range.top);
    long now = System.currentTimeMillis();
    // Reference name has this format: StoreFile#REF_NAME_PARSER
    Path p = new Path(storedir, Long.toString(now) + "." + parent.getEncodedName());
    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    ref.write(fs, p);
    assertFalse(janitor.cleanParent(parent, r));
    // Remove the reference file and try again.
    assertTrue(fs.delete(p, true));
    assertTrue(janitor.cleanParent(parent, r));
  }
}
