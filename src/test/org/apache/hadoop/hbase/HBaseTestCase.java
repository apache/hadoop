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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.regionserver.HRegion;

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

    if (fs == null) {
      this.fs = FileSystem.get(conf);
    }
    try {
      if (localfs) {
        this.testDir = getUnitTestdir(getName());
        if (fs.exists(testDir)) {
          fs.delete(testDir);
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
    
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = filesystem.makeQualified(
        new Path(conf.get(HConstants.HBASE_DIR)));
    filesystem.mkdirs(rootdir);
    
    return HRegion.createHRegion(new HRegionInfo(desc, startKey, endKey),
        rootdir, conf);
  }
  
  protected HRegion openClosedRegion(final HRegion closedRegion)
  throws IOException {
    return new HRegion(closedRegion.getBaseDir(), closedRegion.getLog(),
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
      CompressionType.NONE, false, false, Integer.MAX_VALUE, null));
    htd.addFamily(new HColumnDescriptor(new Text(COLFAMILY_NAME2), versions,
      CompressionType.NONE, false, false, Integer.MAX_VALUE, null));
    htd.addFamily(new HColumnDescriptor(new Text(COLFAMILY_NAME3), versions,
      CompressionType.NONE, false, false, Integer.MAX_VALUE, null));
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
            BatchUpdate batchUpdate = ts == -1 ? 
              new BatchUpdate(t) : new BatchUpdate(t, ts);
            try {
              batchUpdate.put(new Text(column), bytes);
              updater.commit(batchUpdate);
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
    public Cell get(Text row, Text column) throws IOException;
    /**
     * @param row
     * @param column
     * @param versions
     * @return value for row/column pair for number of versions requested
     * @throws IOException
     */
    public Cell[] get(Text row, Text column, int versions) throws IOException;
    /**
     * @param row
     * @param column
     * @param ts
     * @param versions
     * @return value for row/column/timestamp tuple for number of versions
     * @throws IOException
     */
    public Cell[] get(Text row, Text column, long ts, int versions)
    throws IOException;
    /**
     * @param row
     * @param column
     * @param ts
     * @throws IOException
     */
    public void deleteAll(Text row, Text column, long ts) throws IOException;

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
    public HScannerInterface getScanner(Text [] columns, Text firstRow,
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
    
    /** {@inheritDoc} */
    public void commit(BatchUpdate batchUpdate) throws IOException {
      region.batchUpdate(batchUpdate);
    };
    
    /** {@inheritDoc} */
    public void deleteAll(Text row, Text column, long ts) throws IOException {
      this.region.deleteAll(row, column, ts);
    }

    /** {@inheritDoc} */
    public HScannerInterface getScanner(Text [] columns, Text firstRow,
        long ts) throws IOException {
      return this.region.getScanner(columns, firstRow, ts, null);
    }

    /** {@inheritDoc} */
    public Cell get(Text row, Text column) throws IOException {
      return this.region.get(row, column);
    }

    /** {@inheritDoc} */
    public Cell[] get(Text row, Text column, int versions) throws IOException {
      return this.region.get(row, column, versions);
    }

    /** {@inheritDoc} */
    public Cell[] get(Text row, Text column, long ts, int versions)
    throws IOException {
      return this.region.get(row, column, ts, versions);
    }

    /**
     * @param row
     * @return values for each column in the specified row
     * @throws IOException
     */
    public Map<Text, Cell> getFull(Text row) throws IOException {
      return region.getFull(row, null, HConstants.LATEST_TIMESTAMP);
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
    private BatchUpdate batch;
    
    private void checkBatch() {
      if (batch == null) {
        throw new IllegalStateException("No batch update in progress.");
      }
    }
    
    /**
     * @param table
     */
    public HTableIncommon(final HTable table) {
      super();
      this.table = table;
      this.batch = null;
    }
    
    /** {@inheritDoc} */
    public void commit(BatchUpdate batchUpdate) throws IOException {
      table.commit(batchUpdate);
    };
    
    /** {@inheritDoc} */
    public void deleteAll(Text row, Text column, long ts) throws IOException {
      this.table.deleteAll(row, column, ts);
    }
    
    /** {@inheritDoc} */
    public HScannerInterface getScanner(Text [] columns, Text firstRow,
        long ts) throws IOException {
      return this.table.obtainScanner(columns, firstRow, ts, null);
    }
    
    /** {@inheritDoc} */
    public Cell get(Text row, Text column) throws IOException {
      return this.table.get(row, column);
    }
    
    /** {@inheritDoc} */
    public Cell[] get(Text row, Text column, int versions) throws IOException {
      return this.table.get(row, column, versions);
    }
    
    /** {@inheritDoc} */
    public Cell[] get(Text row, Text column, long ts, int versions)
    throws IOException {
      return this.table.get(row, column, ts, versions);
    }
  }
  
  protected void assertCellEquals(final HRegion region, final Text row,
    final Text column, final long timestamp, final String value)
  throws IOException {
    Map<Text, Cell> result = region.getFull(row, null, timestamp);
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
}
