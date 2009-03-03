/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Abstract base class for test cases. Performs all static initialization
 */
public abstract class HBaseTestCase extends TestCase {
  private static final Log LOG = LogFactory.getLog(HBaseTestCase.class);

  /** configuration parameter name for test directory */
  public static final String TEST_DIRECTORY_KEY = "test.build.data";

  protected final static byte [] COLFAMILY_NAME1 = Bytes.toBytes("colfamily1:");
  protected final static byte [] COLFAMILY_NAME2 = Bytes.toBytes("colfamily2:");
  protected final static byte [] COLFAMILY_NAME3 = Bytes.toBytes("colfamily3:");
  protected static final byte [][] COLUMNS = {COLFAMILY_NAME1,
    COLFAMILY_NAME2, COLFAMILY_NAME3};

  private boolean localfs = false;
  protected Path testDir = null;
  protected FileSystem fs = null;
  protected HRegion root = null;
  protected HRegion meta = null;
  protected static final char FIRST_CHAR = 'a';
  protected static final char LAST_CHAR = 'z';
  protected static final String PUNCTUATION = "~`@#$%^&*()-_+=:;',.<>/?[]{}|";
  protected static final byte [] START_KEY_BYTES =
    {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
  protected String START_KEY;
  protected static final int MAXVERSIONS = 3;
  
  static {
    initialize();
  }
  
  protected volatile HBaseConfiguration conf;

  /** constructor */
  public HBaseTestCase() {
    super();
    init();
  }
  
  /**
   * @param name
   */
  public HBaseTestCase(String name) {
    super(name);
    init();
  }
  
  private void init() {
    conf = new HBaseConfiguration();
    try {
      START_KEY = new String(START_KEY_BYTES, HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOG.fatal("error during initialization", e);
      fail();
    }
  }
  
  /**
   * Note that this method must be called after the mini hdfs cluster has
   * started or we end up with a local file system.
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    localfs =
      (conf.get("fs.default.name", "file:///").compareTo("file:///") == 0);

    if (fs == null) {
      this.fs = FileSystem.get(conf);
    }
    try {
      if (localfs) {
        this.testDir = getUnitTestdir(getName());
        if (fs.exists(testDir)) {
          fs.delete(testDir, true);
        }
      } else {
        this.testDir =
          this.fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));
      }
    } catch (Exception e) {
      LOG.fatal("error during setup", e);
      throw e;
    }
  }
  
  @Override
  protected void tearDown() throws Exception {
    try {
      if (localfs) {
        if (this.fs.exists(testDir)) {
          this.fs.delete(testDir, true);
        }
      }
    } catch (Exception e) {
      LOG.fatal("error during tear down", e);
    }
    super.tearDown();
  }

  protected Path getUnitTestdir(String testName) {
    return new Path(
        conf.get(TEST_DIRECTORY_KEY, "test/build/data"), testName);
  }

  protected HRegion createNewHRegion(HTableDescriptor desc, byte [] startKey,
      byte [] endKey)
  throws IOException {
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = filesystem.makeQualified(
        new Path(conf.get(HConstants.HBASE_DIR)));
    filesystem.mkdirs(rootdir);
    
    return HRegion.createHRegion(new HRegionInfo(desc, startKey, endKey),
        rootdir, conf);
  }
  
  protected HRegion openClosedRegion(final HRegion closedRegion)
  throws IOException {
    HRegion r = new HRegion(closedRegion.getBaseDir(), closedRegion.getLog(),
        closedRegion.getFilesystem(), closedRegion.getConf(),
        closedRegion.getRegionInfo(), null);
    r.initialize(null, null);
    return r;
  }
  
  /**
   * Create a table of name <code>name</code> with {@link COLUMNS} for
   * families.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name, MAXVERSIONS);
  }
  
  /**
   * Create a table of name <code>name</code> with {@link COLUMNS} for
   * families.
   * @param name Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name,
      final int versions) {
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME1, versions,
      HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
      Integer.MAX_VALUE, HConstants.FOREVER, false));
    htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME2, versions,
        HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
        Integer.MAX_VALUE, HConstants.FOREVER, false));
    htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME3, versions,
        HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
        Integer.MAX_VALUE,  HConstants.FOREVER, false));
    return htd;
  }
  
  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param r
   * @param column
   * @throws IOException
   * @return count of what we added.
   */
  protected static long addContent(final HRegion r, final byte [] column)
  throws IOException {
    byte [] startKey = r.getRegionInfo().getStartKey();
    byte [] endKey = r.getRegionInfo().getEndKey();
    byte [] startKeyBytes = startKey;
    if (startKeyBytes == null || startKeyBytes.length == 0) {
      startKeyBytes = START_KEY_BYTES;
    }
    return addContent(new HRegionIncommon(r), Bytes.toString(column),
      startKeyBytes, endKey, -1);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param updater  An instance of {@link Incommon}.
   * @param column
   * @throws IOException
   * @return count of what we added.
   */
  protected static long addContent(final Incommon updater, final String column)
  throws IOException {
    return addContent(updater, column, START_KEY_BYTES, null);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param updater  An instance of {@link Incommon}.
   * @param column
   * @param startKeyBytes Where to start the rows inserted
   * @param endKey Where to stop inserting rows.
   * @return count of what we added.
   * @throws IOException
   */
  protected static long addContent(final Incommon updater, final String column,
      final byte [] startKeyBytes, final byte [] endKey)
  throws IOException {
    return addContent(updater, column, startKeyBytes, endKey, -1);
  }
  
  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param updater  An instance of {@link Incommon}.
   * @param column
   * @param startKeyBytes Where to start the rows inserted
   * @param endKey Where to stop inserting rows.
   * @param ts Timestamp to write the content with.
   * @return count of what we added.
   * @throws IOException
   */
  protected static long addContent(final Incommon updater, final String column,
      final byte [] startKeyBytes, final byte [] endKey, final long ts)
  throws IOException {
    long count = 0;
    // Add rows of three characters.  The first character starts with the
    // 'a' character and runs up to 'z'.  Per first character, we run the
    // second character over same range.  And same for the third so rows
    // (and values) look like this: 'aaa', 'aab', 'aac', etc.
    char secondCharStart = (char)startKeyBytes[1];
    char thirdCharStart = (char)startKeyBytes[2];
    EXIT: for (char c = (char)startKeyBytes[0]; c <= LAST_CHAR; c++) {
      for (char d = secondCharStart; d <= LAST_CHAR; d++) {
        for (char e = thirdCharStart; e <= LAST_CHAR; e++) {
          byte [] t = new byte [] {(byte)c, (byte)d, (byte)e};
          if (endKey != null && endKey.length > 0
              && Bytes.compareTo(endKey, t) <= 0) {
            break EXIT;
          }
          try {
            BatchUpdate batchUpdate = ts == -1 ? 
              new BatchUpdate(t) : new BatchUpdate(t, ts);
            try {
              batchUpdate.put(column, t);
              updater.commit(batchUpdate);
              count++;
            } catch (RuntimeException ex) {
              ex.printStackTrace();
              throw ex;
            } catch (IOException ex) {
              ex.printStackTrace();
              throw ex;
            } 
          } catch (RuntimeException ex) {
            ex.printStackTrace();
            throw ex;
          } catch (IOException ex) {
            ex.printStackTrace();
            throw ex;
          }
        }
        // Set start character back to FIRST_CHAR after we've done first loop.
        thirdCharStart = FIRST_CHAR;
      }
      secondCharStart = FIRST_CHAR;
    }
    return count;
  }
  
  /**
   * Implementors can flushcache.
   */
  public static interface FlushCache {
    /**
     * @throws IOException
     */
    public void flushcache() throws IOException;
  }
  
  /**
   * Interface used by tests so can do common operations against an HTable
   * or an HRegion.
   * 
   * TOOD: Come up w/ a better name for this interface.
   */
  public static interface Incommon {
    /**
     * @param row
     * @param column
     * @return value for row/column pair
     * @throws IOException
     */
    public Cell get(byte [] row, byte [] column) throws IOException;
    /**
     * @param row
     * @param column
     * @param versions
     * @return value for row/column pair for number of versions requested
     * @throws IOException
     */
    public Cell[] get(byte [] row, byte [] column, int versions) throws IOException;
    /**
     * @param row
     * @param column
     * @param ts
     * @param versions
     * @return value for row/column/timestamp tuple for number of versions
     * @throws IOException
     */
    public Cell[] get(byte [] row, byte [] column, long ts, int versions)
    throws IOException;
    /**
     * @param row
     * @param column
     * @param ts
     * @throws IOException
     */
    public void deleteAll(byte [] row, byte [] column, long ts) throws IOException;

    /**
     * @param batchUpdate
     * @throws IOException
     */
    public void commit(BatchUpdate batchUpdate) throws IOException;

    /**
     * @param columns
     * @param firstRow
     * @param ts
     * @return scanner for specified columns, first row and timestamp
     * @throws IOException
     */
    public ScannerIncommon getScanner(byte [] [] columns, byte [] firstRow,
      long ts) throws IOException;
  }
  
  /**
   * A class that makes a {@link Incommon} out of a {@link HRegion}
   */
  public static class HRegionIncommon implements Incommon, FlushCache {
    final HRegion region;
    private BatchUpdate batch;
    
    private void checkBatch() {
      if (batch == null) {
        throw new IllegalStateException("No update in progress");
      }
    }
    
    /**
     * @param HRegion
     */
    public HRegionIncommon(final HRegion HRegion) {
      this.region = HRegion;
      this.batch = null;
    }
    
    public void commit(BatchUpdate batchUpdate) throws IOException {
      region.batchUpdate(batchUpdate, null);
    }
    
    public void deleteAll(byte [] row, byte [] column, long ts)
    throws IOException {
      this.region.deleteAll(row, column, ts, null);
    }

    public ScannerIncommon getScanner(byte [][] columns, byte [] firstRow,
      long ts) 
    throws IOException {
      return new 
        InternalScannerIncommon(region.getScanner(columns, firstRow, ts, null));
    }

    public Cell get(byte [] row, byte [] column) throws IOException {
      Cell[] result = this.region.get(row, column, -1, -1);
      return (result == null)? null : result[0];
    }

    public Cell[] get(byte [] row, byte [] column, int versions)
    throws IOException {
      return this.region.get(row, column, -1, versions);
    }

    public Cell[] get(byte [] row, byte [] column, long ts, int versions)
    throws IOException {
      return this.region.get(row, column, ts, versions);
    }

    /**
     * @param row
     * @return values for each column in the specified row
     * @throws IOException
     */
    public Map<byte [], Cell> getFull(byte [] row) throws IOException {
      return region.getFull(row, null, HConstants.LATEST_TIMESTAMP, 1, null);
    }

    public void flushcache() throws IOException {
      this.region.flushcache();
    }
  }

  /**
   * A class that makes a {@link Incommon} out of a {@link HTable}
   */
  public static class HTableIncommon implements Incommon {
    final HTable table;
    private BatchUpdate batch;

    /**
     * @param table
     */
    public HTableIncommon(final HTable table) {
      super();
      this.table = table;
      this.batch = null;
    }
    
    public void commit(BatchUpdate batchUpdate) throws IOException {
      table.commit(batchUpdate);
    }
    
    public void deleteAll(byte [] row, byte [] column, long ts)
    throws IOException {
      this.table.deleteAll(row, column, ts);
    }
    
    public ScannerIncommon getScanner(byte [][] columns, byte [] firstRow, long ts) 
    throws IOException {
      return new 
        ClientScannerIncommon(table.getScanner(columns, firstRow, ts, null));
    }
    
    public Cell get(byte [] row, byte [] column) throws IOException {
      return this.table.get(row, column);
    }
    
    public Cell[] get(byte [] row, byte [] column, int versions)
    throws IOException {
      return this.table.get(row, column, versions);
    }
    
    public Cell[] get(byte [] row, byte [] column, long ts, int versions)
    throws IOException {
      return this.table.get(row, column, ts, versions);
    }
  }
  
  public interface ScannerIncommon 
  extends Iterable<Map.Entry<HStoreKey, SortedMap<byte [], Cell>>> {
    public boolean next(HStoreKey key, SortedMap<byte [], Cell> values)
    throws IOException;
    
    public void close() throws IOException;
  }
  
  public static class ClientScannerIncommon implements ScannerIncommon {
    Scanner scanner;
    public ClientScannerIncommon(Scanner scanner) {
      this.scanner = scanner;
    }
    
    public boolean next(HStoreKey key, SortedMap<byte [], Cell> values)
    throws IOException {
      RowResult results = scanner.next();
      if (results == null) {
        return false;
      }
      key.setRow(results.getRow());
      values.clear();
      for (Map.Entry<byte [], Cell> entry : results.entrySet()) {
        values.put(entry.getKey(), entry.getValue());
      }
      return true;
    }
    
    public void close() throws IOException {
      scanner.close();
    }
    
    public Iterator iterator() {
      return scanner.iterator();
    }
  }
  
  public static class InternalScannerIncommon implements ScannerIncommon {
    InternalScanner scanner;
    
    public InternalScannerIncommon(InternalScanner scanner) {
      this.scanner = scanner;
    }
    
    public boolean next(HStoreKey key, SortedMap<byte [], Cell> values)
    throws IOException {
      return scanner.next(key, values);
    }
    
    public void close() throws IOException {
      scanner.close();
    }
    
    public Iterator iterator() {
      throw new UnsupportedOperationException();
    }
  }
  
  protected void assertCellEquals(final HRegion region, final byte [] row,
    final byte [] column, final long timestamp, final String value)
  throws IOException {
    Map<byte [], Cell> result = region.getFull(row, null, timestamp, 1, null);
    Cell cell_value = result.get(column);
    if(value == null){
      assertEquals(column.toString() + " at timestamp " + timestamp, null, cell_value);
    } else {
      if (cell_value == null) {
        fail(column.toString() + " at timestamp " + timestamp + 
          "\" was expected to be \"" + value + " but was null");
      }
      if (cell_value != null) {
        assertEquals(column.toString() + " at timestamp " 
            + timestamp, value, new String(cell_value.getValue()));
      }
    }
  }
  
  /**
   * Initializes parameters used in the test environment:
   * 
   * Sets the configuration parameter TEST_DIRECTORY_KEY if not already set.
   * Sets the boolean debugging if "DEBUGGING" is set in the environment.
   * If debugging is enabled, reconfigures loggin so that the root log level is
   * set to WARN and the logging level for the package is set to DEBUG.
   */
  @SuppressWarnings("unchecked")
  public static void initialize() {
    if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
      System.setProperty(TEST_DIRECTORY_KEY, new File(
          "build/hbase/test").getAbsolutePath());
    }
  }

  /**
   * Common method to close down a MiniDFSCluster and the associated file system
   * 
   * @param cluster
   */
  public static void shutdownDfs(MiniDFSCluster cluster) {
    if (cluster != null) {
      try {
        FileSystem fs = cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }

      LOG.info("Shutting down Mini DFS ");
      try {
        cluster.shutdown();
      } catch (Exception e) {
        /// Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
    }
  }
  
  protected void createRootAndMetaRegions() throws IOException {
    root = HRegion.createHRegion(HRegionInfo.ROOT_REGIONINFO, testDir, conf);
    meta = HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO, testDir, 
        conf);
    HRegion.addRegionToMETA(root, meta);
  }
  
  protected void closeRootAndMeta() throws IOException {
    if (meta != null) {
      meta.close();
      meta.getLog().closeAndDelete();
    }
    if (root != null) {
      root.close();
      root.getLog().closeAndDelete();
    }
  }
}
