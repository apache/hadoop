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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.Text;

/**
 * Abstract base class for test cases. Performs all static initialization
 */
public abstract class HBaseTestCase extends TestCase {
  private static final Log LOG = LogFactory.getLog(HBaseTestCase.class);

  protected final static String COLFAMILY_NAME1 = "colfamily1:";
  protected final static String COLFAMILY_NAME2 = "colfamily2:";
  protected final static String COLFAMILY_NAME3 = "colfamily3:";
  protected static Text [] COLUMNS = new Text [] {new Text(COLFAMILY_NAME1),
    new Text(COLFAMILY_NAME2), new Text(COLFAMILY_NAME3)};
  private boolean localfs = false;
  protected Path testDir = null;
  protected FileSystem fs = null;
  protected static final char FIRST_CHAR = 'a';
  protected static final char LAST_CHAR = 'z';
  protected static final String PUNCTUATION = "~`@#$%^&*()-_+=:;',.<>/?[]{}|";
  protected static final byte [] START_KEY_BYTES =
    {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
  protected String START_KEY;
  protected static final int MAXVERSIONS = 3;
  
  static {
    StaticTestEnvironment.initialize();
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
      START_KEY =
        new String(START_KEY_BYTES, HConstants.UTF8_ENCODING) + PUNCTUATION;
    } catch (UnsupportedEncodingException e) {
      LOG.fatal("error during initialization", e);
      fail();
    }
  }
  
  /**
   * {@inheritDoc}
   * 
   * Note that this method must be called after the mini hdfs cluster has
   * started or we end up with a local file system.
   * 
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    localfs =
      (conf.get("fs.default.name", "file:///").compareTo("file::///") == 0);

    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.fatal("error getting file system", e);
      throw e;
    }
    try {
      if (localfs) {
        this.testDir = getUnitTestdir(getName());
        if (fs.exists(testDir)) {
          fs.delete(testDir);
        }
      } else {
        this.testDir = fs.makeQualified(
            new Path(conf.get(HConstants.HBASE_DIR, HConstants.DEFAULT_HBASE_DIR))
        );
      }
    } catch (Exception e) {
      LOG.fatal("error during setup", e);
      throw e;
    }
  }
  
  /** {@inheritDoc} */
  @Override
  protected void tearDown() throws Exception {
    try {
      if (localfs) {
        if (this.fs.exists(testDir)) {
          this.fs.delete(testDir);
        }
      }
    } catch (Exception e) {
      LOG.fatal("error during tear down", e);
    }
    super.tearDown();
  }

  protected Path getUnitTestdir(String testName) {
    return new Path(
        conf.get(StaticTestEnvironment.TEST_DIRECTORY_KEY, "test/build/data"),
        testName);
  }

  protected HRegion createNewHRegion(HTableDescriptor desc, Text startKey,
      Text endKey) throws IOException {
    
    FileSystem fs = FileSystem.get(conf);
    Path rootdir = fs.makeQualified(
        new Path(conf.get(HConstants.HBASE_DIR, HConstants.DEFAULT_HBASE_DIR)));
    fs.mkdirs(rootdir);
    
    return HRegion.createHRegion(new HRegionInfo(desc, startKey, endKey),
        rootdir, conf);
  }
  
  protected HRegion openClosedRegion(final HRegion closedRegion)
  throws IOException {
    return new HRegion(closedRegion.basedir, closedRegion.getLog(),
      closedRegion.getFilesystem(), closedRegion.getConf(),
      closedRegion.getRegionInfo(), null, null);
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
    htd.addFamily(new HColumnDescriptor(new Text(COLFAMILY_NAME1), versions,
      CompressionType.NONE, false,  Integer.MAX_VALUE, null));
    htd.addFamily(new HColumnDescriptor(new Text(COLFAMILY_NAME2), versions,
      CompressionType.NONE, false,  Integer.MAX_VALUE, null));
    htd.addFamily(new HColumnDescriptor(new Text(COLFAMILY_NAME3), versions,
      CompressionType.NONE, false,  Integer.MAX_VALUE, null));
    return htd;
  }
  
  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param r
   * @param column
   * @throws IOException
   */
  protected static void addContent(final HRegion r, final String column)
  throws IOException {
    Text startKey = r.getRegionInfo().getStartKey();
    Text endKey = r.getRegionInfo().getEndKey();
    byte [] startKeyBytes = startKey.getBytes();
    if (startKeyBytes == null || startKeyBytes.length == 0) {
      startKeyBytes = START_KEY_BYTES;
    }
    addContent(new HRegionIncommon(r), column, startKeyBytes, endKey, -1);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param updater  An instance of {@link Incommon}.
   * @param column
   * @throws IOException
   */
  protected static void addContent(final Incommon updater, final String column)
  throws IOException {
    addContent(updater, column, START_KEY_BYTES, null);
  }

  /**
   * Add content to region <code>r</code> on the passed column
   * <code>column</code>.
   * Adds data of the from 'aaa', 'aab', etc where key and value are the same.
   * @param updater  An instance of {@link Incommon}.
   * @param column
   * @param startKeyBytes Where to start the rows inserted
   * @param endKey Where to stop inserting rows.
   * @throws IOException
   */
  protected static void addContent(final Incommon updater, final String column,
      final byte [] startKeyBytes, final Text endKey)
  throws IOException {
    addContent(updater, column, startKeyBytes, endKey, -1);
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
   * @throws IOException
   */
  protected static void addContent(final Incommon updater, final String column,
      final byte [] startKeyBytes, final Text endKey, final long ts)
  throws IOException {
    // Add rows of three characters.  The first character starts with the
    // 'a' character and runs up to 'z'.  Per first character, we run the
    // second character over same range.  And same for the third so rows
    // (and values) look like this: 'aaa', 'aab', 'aac', etc.
    char secondCharStart = (char)startKeyBytes[1];
    char thirdCharStart = (char)startKeyBytes[2];
    EXIT: for (char c = (char)startKeyBytes[0]; c <= LAST_CHAR; c++) {
      for (char d = secondCharStart; d <= LAST_CHAR; d++) {
        for (char e = thirdCharStart; e <= LAST_CHAR; e++) {
          byte [] bytes = new byte [] {(byte)c, (byte)d, (byte)e};
          String s = new String(bytes, HConstants.UTF8_ENCODING) + PUNCTUATION;
          bytes = s.getBytes(HConstants.UTF8_ENCODING);
          Text t = new Text(s);
          if (endKey != null && endKey.getLength() > 0
              && endKey.compareTo(t) <= 0) {
            break EXIT;
          }
          try {
            long lockid = updater.startBatchUpdate(t);
            try {
              updater.put(lockid, new Text(column), bytes);
              if (ts == -1) {
                updater.commit(lockid);
              } else {
                updater.commit(lockid, ts);
              }
              lockid = -1;
            } catch (RuntimeException ex) {
              ex.printStackTrace();
              throw ex;
              
            } catch (IOException ex) {
              ex.printStackTrace();
              throw ex;
              
            } finally {
              if (lockid != -1) {
                updater.abort(lockid);
              }
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
    public byte [] get(Text row, Text column) throws IOException;
    /**
     * @param row
     * @param column
     * @param versions
     * @return value for row/column pair for number of versions requested
     * @throws IOException
     */
    public byte [][] get(Text row, Text column, int versions) throws IOException;
    /**
     * @param row
     * @param column
     * @param ts
     * @param versions
     * @return value for row/column/timestamp tuple for number of versions
     * @throws IOException
     */
    public byte [][] get(Text row, Text column, long ts, int versions)
    throws IOException;
    /**
     * @param row
     * @return batch update identifier
     * @throws IOException
     */
    public long startBatchUpdate(final Text row) throws IOException;
    /**
     * @param lockid
     * @param column
     * @param val
     * @throws IOException
     */
    public void put(long lockid, Text column, byte val[]) throws IOException;
    /**
     * @param lockid
     * @param column
     * @throws IOException
     */
    public void delete(long lockid, Text column) throws IOException;
    /**
     * @param row
     * @param column
     * @param ts
     * @throws IOException
     */
    public void deleteAll(Text row, Text column, long ts) throws IOException;
    /**
     * @param lockid
     * @throws IOException
     */
    public void commit(long lockid) throws IOException;
    /**
     * @param lockid
     * @param ts
     * @throws IOException
     */
    public void commit(long lockid, long ts) throws IOException;
    /**
     * @param lockid
     * @throws IOException
     */
    public void abort(long lockid) throws IOException;
    /**
     * @param columns
     * @param firstRow
     * @param ts
     * @return scanner for specified columns, first row and timestamp
     * @throws IOException
     */
    public HScannerInterface getScanner(Text [] columns, Text firstRow,
      long ts) throws IOException;
  }
  
  /**
   * A class that makes a {@link Incommon} out of a {@link HRegion}
   */
  public static class HRegionIncommon implements Incommon, FlushCache {
    final HRegion region;
    private final Random rand = new Random();
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
    /** {@inheritDoc} */
    public void abort(@SuppressWarnings("unused") long lockid) {
      this.batch = null;
    }
    /** {@inheritDoc} */
    public void commit(long lockid) throws IOException {
      commit(lockid, HConstants.LATEST_TIMESTAMP);
    }
    /** {@inheritDoc} */
    public void commit(@SuppressWarnings("unused") long lockid, final long ts)
    throws IOException {
      checkBatch();
      try {
        this.region.batchUpdate(ts, batch);
      } finally {
        this.batch = null;
      }
    }
    /** {@inheritDoc} */
    public void put(long lockid, Text column, byte[] val) {
      checkBatch();
      this.batch.put(lockid, column, val);
    }
    /** {@inheritDoc} */
    public void delete(long lockid, Text column) {
      checkBatch();
      this.batch.delete(lockid, column);
    }
    /** {@inheritDoc} */
    public void deleteAll(Text row, Text column, long ts) throws IOException {
      this.region.deleteAll(row, column, ts);
    }
    /** {@inheritDoc} */
    public long startBatchUpdate(Text row) {
      return startUpdate(row);
    }
    /**
     * @param row
     * @return update id
     */
    public long startUpdate(Text row) {
      if (this.batch != null) {
        throw new IllegalStateException("Update already in progress");
      }
      long lockid = Math.abs(rand.nextLong());
      this.batch = new BatchUpdate(lockid);
      return batch.startUpdate(row);
    }
    /** {@inheritDoc} */
    public HScannerInterface getScanner(Text [] columns, Text firstRow,
        long ts) throws IOException {
      return this.region.getScanner(columns, firstRow, ts, null);
    }
    /** {@inheritDoc} */
    public byte[] get(Text row, Text column) throws IOException {
      return this.region.get(row, column);
    }
    /** {@inheritDoc} */
    public byte[][] get(Text row, Text column, int versions) throws IOException {
      return this.region.get(row, column, versions);
    }
    /** {@inheritDoc} */
    public byte[][] get(Text row, Text column, long ts, int versions)
        throws IOException {
      return this.region.get(row, column, ts, versions);
    }
    /**
     * @param row
     * @return values for each column in the specified row
     * @throws IOException
     */
    public Map<Text, byte []> getFull(Text row) throws IOException {
      return region.getFull(row);
    }
    /** {@inheritDoc} */
    public void flushcache() throws IOException {
      this.region.flushcache();
    }
  }

  /**
   * A class that makes a {@link Incommon} out of a {@link HTable}
   */
  public static class HTableIncommon implements Incommon {
    final HTable table;
    /**
     * @param table
     */
    public HTableIncommon(final HTable table) {
      super();
      this.table = table;
    }
    /** {@inheritDoc} */
    public void abort(long lockid) {
      this.table.abort(lockid);
    }
    /** {@inheritDoc} */
    public void commit(long lockid) throws IOException {
      this.table.commit(lockid);
    }
    /** {@inheritDoc} */
    public void commit(long lockid, final long ts) throws IOException {
      this.table.commit(lockid, ts);
    }
    /** {@inheritDoc} */
    public void put(long lockid, Text column, byte[] val) {
      this.table.put(lockid, column, val);
    }
    /** {@inheritDoc} */
    public void delete(long lockid, Text column) {
      this.table.delete(lockid, column);
    }
    /** {@inheritDoc} */
    public void deleteAll(Text row, Text column, long ts) throws IOException {
      this.table.deleteAll(row, column, ts);
    }
    /** {@inheritDoc} */
    public long startBatchUpdate(Text row) {
      return this.table.startUpdate(row);
    }
    /** {@inheritDoc} */
    public HScannerInterface getScanner(Text [] columns, Text firstRow,
        long ts) throws IOException {
      return this.table.obtainScanner(columns, firstRow, ts, null);
    }
    /** {@inheritDoc} */
    public byte[] get(Text row, Text column) throws IOException {
      return this.table.get(row, column);
    }
    /** {@inheritDoc} */
    public byte[][] get(Text row, Text column, int versions) throws IOException {
      return this.table.get(row, column, versions);
    }
    /** {@inheritDoc} */
    public byte[][] get(Text row, Text column, long ts, int versions)
    throws IOException {
      return this.table.get(row, column, ts, versions);
    }
  }
}
