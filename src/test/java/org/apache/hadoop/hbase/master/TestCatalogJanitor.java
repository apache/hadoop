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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
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
      Path rootdir = fs.makeQualified(new Path(this.c.get(HConstants.HBASE_DIR)));
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
      this.mfs = new MasterFileSystem(server, this, null);
      this.asm = Mockito.mock(AssignmentManager.class);
    }

    @Override
    public void checkTableModifiable(byte[] tableName) throws IOException {
      //no-op
    }

    @Override
    public void createTable(HTableDescriptor desc, byte[][] splitKeys)
        throws IOException {
      // no-op
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

    @Override
    public TableDescriptors getTableDescriptors() {
      return new TableDescriptors() {
        @Override
        public HTableDescriptor remove(String tablename) throws IOException {
          // TODO Auto-generated method stub
          return null;
        }
        
        @Override
        public Map<String, HTableDescriptor> getAll() throws IOException {
          // TODO Auto-generated method stub
          return null;
        }
        
        @Override
        public HTableDescriptor get(byte[] tablename)
        throws TableExistsException, FileNotFoundException, IOException {
          return get(Bytes.toString(tablename));
        }
        
        @Override
        public HTableDescriptor get(String tablename)
        throws TableExistsException, FileNotFoundException, IOException {
          return createHTableDescriptor();
        }
        
        @Override
        public void add(HTableDescriptor htd) throws IOException {
          // TODO Auto-generated method stub
          
        }
      };
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
    setRootDirAndCleanIt(htu, "testCleanParent");
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);
    CatalogJanitor janitor = new CatalogJanitor(server, services);
    // Create regions.
    HTableDescriptor htd = createHTableDescriptor();
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
    Result r = createResult(parent, splita, splitb);
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

  /**
   * Make sure parent gets cleaned up even if daughter is cleaned up before it.
   * @throws IOException
   * @throws InterruptedException 
   */
  @Test
  public void testParentCleanedEvenIfDaughterGoneFirst()
  throws IOException, InterruptedException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    setRootDirAndCleanIt(htu, "testParentCleanedEvenIfDaughterGoneFirst");
    Server server = new MockServer(htu);
    MasterServices services = new MockMasterServices(server);
    CatalogJanitor janitor = new CatalogJanitor(server, services);
    final HTableDescriptor htd = createHTableDescriptor();

    // Create regions: aaa->eee, aaa->ccc, aaa->bbb, bbb->ccc, etc.

    // Parent
    HRegionInfo parent = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("eee"));
    // Sleep a second else the encoded name on these regions comes out
    // same for all with same start key and made in same second.
    Thread.sleep(1001);

    // Daughter a
    HRegionInfo splita = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("ccc"));
    Thread.sleep(1001);
    // Make daughters of daughter a; splitaa and splitab.
    HRegionInfo splitaa = new HRegionInfo(htd.getName(), Bytes.toBytes("aaa"),
      Bytes.toBytes("bbb"));
    HRegionInfo splitab = new HRegionInfo(htd.getName(), Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"));

    // Daughter b
    HRegionInfo splitb = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
      Bytes.toBytes("eee"));
    Thread.sleep(1001);
    // Make Daughters of daughterb; splitba and splitbb.
    HRegionInfo splitba = new HRegionInfo(htd.getName(), Bytes.toBytes("ccc"),
      Bytes.toBytes("ddd"));
    HRegionInfo splitbb = new HRegionInfo(htd.getName(), Bytes.toBytes("ddd"),
      Bytes.toBytes("eee"));

    // First test that our Comparator works right up in CatalogJanitor.
    // Just fo kicks.
    SortedMap<HRegionInfo, Result> regions =
      new TreeMap<HRegionInfo, Result>(new CatalogJanitor.SplitParentFirstComparator());
    // Now make sure that this regions map sorts as we expect it to.
    regions.put(parent, createResult(parent, splita, splitb));
    regions.put(splitb, createResult(splitb, splitba, splitbb));
    regions.put(splita, createResult(splita, splitaa, splitab));
    // Assert its properly sorted.
    int index = 0;
    for (Map.Entry<HRegionInfo, Result> e: regions.entrySet()) {
      if (index == 0) {
        assertTrue(e.getKey().getEncodedName().equals(parent.getEncodedName()));
      } else if (index == 1) {
        assertTrue(e.getKey().getEncodedName().equals(splita.getEncodedName()));
      } else if (index == 2) {
        assertTrue(e.getKey().getEncodedName().equals(splitb.getEncodedName()));
      }
      index++;
    }

    // Now play around with the cleanParent function.  Create a ref from splita
    // up to the parent.
    Path splitaRef =
      createReferences(services, htd, parent, splita, Bytes.toBytes("ccc"), false);
    // Make sure actual super parent sticks around because splita has a ref.
    assertFalse(janitor.cleanParent(parent, regions.get(parent)));

    //splitba, and split bb, do not have dirs in fs.  That means that if
    // we test splitb, it should get cleaned up.
    assertTrue(janitor.cleanParent(splitb, regions.get(splitb)));

    // Now remove ref from splita to parent... so parent can be let go and so
    // the daughter splita can be split (can't split if still references).
    // BUT make the timing such that the daughter gets cleaned up before we
    // can get a chance to let go of the parent.
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    assertTrue(fs.delete(splitaRef, true));
    // Create the refs from daughters of splita.
    Path splitaaRef =
      createReferences(services, htd, splita, splitaa, Bytes.toBytes("bbb"), false);
    Path splitabRef =
      createReferences(services, htd, splita, splitab, Bytes.toBytes("bbb"), true);

    // Test splita.  It should stick around because references from splitab, etc.
    assertFalse(janitor.cleanParent(splita, regions.get(splita)));

    // Now clean up parent daughter first.  Remove references from its daughters.
    assertTrue(fs.delete(splitaaRef, true));
    assertTrue(fs.delete(splitabRef, true));
    assertTrue(janitor.cleanParent(splita, regions.get(splita)));

    // Super parent should get cleaned up now both splita and splitb are gone.
    assertTrue(janitor.cleanParent(parent, regions.get(parent)));
  }

  private String setRootDirAndCleanIt(final HBaseTestingUtility htu,
      final String subdir)
  throws IOException {
    Path testdir = HBaseTestingUtility.getTestDir(subdir);
    FileSystem fs = FileSystem.get(htu.getConfiguration());
    if (fs.exists(testdir)) assertTrue(fs.delete(testdir, true));
    htu.getConfiguration().set(HConstants.HBASE_DIR, testdir.toString());
    return htu.getConfiguration().get(HConstants.HBASE_DIR);
  }

  /**
   * @param services Master services instance.
   * @param htd
   * @param parent
   * @param daughter
   * @param midkey
   * @param top True if we are to write a 'top' reference.
   * @return Path to reference we created.
   * @throws IOException
   */
  private Path createReferences(final MasterServices services,
      final HTableDescriptor htd, final HRegionInfo parent,
      final HRegionInfo daughter, final byte [] midkey, final boolean top)
  throws IOException {
    Path rootdir = services.getMasterFileSystem().getRootDir();
    Path tabledir = HTableDescriptor.getTableDir(rootdir, parent.getTableName());
    Path storedir = Store.getStoreHomedir(tabledir, daughter.getEncodedName(),
      htd.getColumnFamilies()[0].getName());
    Reference ref = new Reference(midkey,
      top? Reference.Range.top: Reference.Range.bottom);
    long now = System.currentTimeMillis();
    // Reference name has this format: StoreFile#REF_NAME_PARSER
    Path p = new Path(storedir, Long.toString(now) + "." + parent.getEncodedName());
    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    ref.write(fs, p);
    return p;
  }

  private Result createResult(final HRegionInfo parent, final HRegionInfo a,
      final HRegionInfo b)
  throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
      HConstants.SPLITA_QUALIFIER, Writables.getBytes(a)));
    kvs.add(new KeyValue(parent.getRegionName(), HConstants.CATALOG_FAMILY,
      HConstants.SPLITB_QUALIFIER, Writables.getBytes(b)));
    return new Result(kvs);
  }

  private HTableDescriptor createHTableDescriptor() {
    HTableDescriptor htd = new HTableDescriptor("t");
    htd.addFamily(new HColumnDescriptor("f"));
    return htd;
  }
}