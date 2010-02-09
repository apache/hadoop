/**
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;


/**
 * Used to communicate with a single HBase table.
 * <p>
 * This class is not thread safe for writes.
 * Gets, puts, and deletes take out a row lock for the duration
 * of their operation.  Scans (currently) do not respect
 * row locking.
 */
public class HTable implements HTableInterface {
  private final HConnection connection;
  private final byte [] tableName;
  protected final int scannerTimeout;
  private volatile Configuration configuration;
  private final ArrayList<Put> writeBuffer = new ArrayList<Put>();
  private long writeBufferSize;
  private boolean autoFlush;
  private long currentWriteBufferSize;
  protected int scannerCaching;
  private long maxScannerResultSize;
  private int maxKeyValueSize;

  /**
   * Creates an object to access a HBase table
   *
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(final String tableName)
  throws IOException {
    this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table
   *
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(final byte [] tableName)
  throws IOException {
    this(HBaseConfiguration.create(), tableName);
  }

  /**
   * Creates an object to access a HBase table
   *
   * @param conf configuration object
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table.
   *
   * @param conf configuration object
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(Configuration conf, final byte [] tableName)
  throws IOException {
    this.tableName = tableName;
    if (conf == null) {
      this.scannerTimeout = 0;
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.scannerTimeout =
      (int) conf.getLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);
    this.configuration = conf;
    this.connection.locateRegion(tableName, HConstants.EMPTY_START_ROW);
    this.writeBufferSize = conf.getLong("hbase.client.write.buffer", 2097152);
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = conf.getInt("hbase.client.scanner.caching", 1);
    this.maxScannerResultSize = conf.getLong(
      HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    this.maxKeyValueSize = conf.getInt("hbase.client.keyvalue.maxsize", -1);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(HBaseConfiguration.create(), tableName);
  }

  /**
   * @param conf HBaseConfiguration object
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(Configuration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, Bytes.toBytes(tableName));
  }

  /**
   * @param conf HBaseConfiguration object
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public static boolean isTableEnabled(Configuration conf, byte[] tableName)
  throws IOException {
    return HConnectionManager.getConnection(conf).isTableEnabled(tableName);
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   * @throws IOException
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   * @throws IOException
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /** @return the table name */
  public byte [] getTableName() {
    return this.tableName;
  }

  /**
   * Used by unit tests and tools to do low-level manipulations.  Not for
   * general use.
   * @return An HConnection instance.
   */
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * Get the number of rows for caching that will be passed to scanners
   * @return the number of rows for caching
   */
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * Set the number of rows for caching that will be passed to scanners
   * @param scannerCaching the number of rows for caching
   */
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  /**
   * @return table metadata
   * @throws IOException
   */
  public HTableDescriptor getTableDescriptor() throws IOException {
    return new UnmodifyableHTableDescriptor(
      this.connection.getHTableDescriptor(this.tableName));
  }

  /**
   * Gets the starting row key for every region in the currently open table
   *
   * @return Array of region starting row keys
   * @throws IOException
   */
  public byte [][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * Gets the ending row key for every region in the currently open table
   *
   * @return Array of region ending row keys
   * @throws IOException
   */
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table
   *
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    final List<byte[]> startKeyList = new ArrayList<byte[]>();
    final List<byte[]> endKeyList = new ArrayList<byte[]>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
        if (Bytes.equals(info.getTableDesc().getName(), getTableName())) {
          if (!(info.isOffline() || info.isSplit())) {
            startKeyList.add(info.getStartKey());
            endKeyList.add(info.getEndKey());
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(configuration, visitor, this.tableName);
    return new Pair(startKeyList.toArray(new byte[startKeyList.size()][]),
                endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  /**
   * Get all the regions and their address for this table
   *
   * @return A map of HRegionInfo with it's server address
   * @throws IOException
   */
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    final Map<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));

        if (!(Bytes.equals(info.getTableDesc().getName(), getTableName()))) {
          return false;
        }

        HServerAddress server = new HServerAddress();
        byte [] value = rowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.SERVER_QUALIFIER);
        if (value != null && value.length > 0) {
          String address = Bytes.toString(value);
          server = new HServerAddress(address);
        }

        if (!(info.isOffline() || info.isSplit())) {
          regionMap.put(new UnmodifyableHRegionInfo(info), server);
        }
        return true;
      }

    };
    MetaScanner.metaScan(configuration, visitor, tableName);
    return regionMap;
  }

  /**
   * Return the row that matches <i>row</i> exactly,
   * or the one that immediately preceeds it.
   *
   * @param row row key
   * @param family Column family to look for row in.
   * @return map of values
   * @throws IOException
   * @since 0.20.0
    */
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     return connection.getRegionServerWithRetries(
         new ServerCallable<Result>(connection, tableName, row) {
       public Result call() throws IOException {
         return server.getClosestRowBefore(location.getRegionInfo().getRegionName(),
           row, family);
       }
     });
   }

  /**
   * Get a scanner on the current table as specified by the {@link Scan} object
   *
   * @param scan a configured {@link Scan} object
   * @return scanner
   * @throws IOException
   * @since 0.20.0
   */
  public ResultScanner getScanner(final Scan scan) throws IOException {
    ClientScanner s = new ClientScanner(scan);
    s.initialize();
    return s;
  }
  /**
   * Get a scanner on the current table as specified by the {@link Scan} object
   *
   * @param family  The column family to scan.
   * @return The scanner.
   * @throws IOException
   * @since 0.20.0
   */
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * Get a scanner on the current table as specified by the {@link Scan} object
   *
   * @param family  The column family to scan.
   * @param qualifier  The column qualifier to scan.
   * @return The scanner.
   * @throws IOException
   * @since 0.20.0
   */
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * Method for getting data from a row
   * @param get the Get to fetch
   * @return the result
   * @throws IOException
   * @since 0.20.0
   */
  public Result get(final Get get) throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, get.getRow()) {
          public Result call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  /**
   *
   * @param delete
   * @throws IOException
   * @since 0.20.0
   */
  public void delete(final Delete delete)
  throws IOException {
    connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, delete.getRow()) {
          public Boolean call() throws IOException {
            server.delete(location.getRegionInfo().getRegionName(), delete);
            return null; // FindBugs NP_BOOLEAN_RETURN_NULL
          }
        }
    );
  }

  /**
   * Bulk commit a List of Deletes to the table.
   * @param deletes List of deletes.  List is modified by this method.  On
   * exception holds deletes that were NOT applied.
   * @throws IOException
   * @since 0.20.1
   */
  public synchronized void delete(final List<Delete> deletes)
  throws IOException {
    int last = 0;
    try {
      last = connection.processBatchOfDeletes(deletes, this.tableName);
    } finally {
      deletes.subList(0, last).clear();
    }
  }

  /**
   * Commit a Put to the table.
   * <p>
   * If autoFlush is false, the update is buffered.
   * @param put
   * @throws IOException
   * @since 0.20.0
   */
  public synchronized void put(final Put put) throws IOException {
    validatePut(put);
    writeBuffer.add(put);
    currentWriteBufferSize += put.heapSize();
    if(autoFlush || currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * Commit a List of Puts to the table.
   * <p>
   * If autoFlush is false, the update is buffered.
   * @param puts
   * @throws IOException
   * @since 0.20.0
   */
  public synchronized void put(final List<Put> puts) throws IOException {
    for (Put put : puts) {
      validatePut(put);
      writeBuffer.add(put);
      currentWriteBufferSize += put.heapSize();
    }
    if (autoFlush || currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception.<p>
   *
   * @param row
   * @param family
   * @param qualifier
   * @param amount
   * @return The new value.
   * @throws IOException
   */
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception.<p>
   *
   * Setting writeToWAL to false means that in a fail scenario, you will lose
   * any increments that have not been flushed.
   * @param row
   * @param family
   * @param qualifier
   * @param amount
   * @param writeToWAL true if increment should be applied to WAL, false if not
   * @return The new value.
   * @throws IOException
   */
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("column is null");
    }
    if (npe != null) {
      IOException io = new IOException(
          "Invalid arguments to incrementColumnValue", npe);
      throw io;
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Long>(connection, tableName, row) {
          public Long call() throws IOException {
            return server.incrementColumnValue(
                location.getRegionInfo().getRegionName(), row, family,
                qualifier, amount, writeToWAL);
          }
        }
    );
  }

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the put.
   *
   * @param row
   * @param family
   * @param qualifier
   * @param value the expected value
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public synchronized boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row) {
          public Boolean call() throws IOException {
            return server.checkAndPut(location.getRegionInfo().getRegionName(),
              row, family, qualifier, value, put)? Boolean.TRUE: Boolean.FALSE;
          }
        }
      ).booleanValue();
  }

  /**
   * Test for the existence of columns in the table, as specified in the Get.<p>
   *
   * This will return true if the Get matches one or more keys, false if not.<p>
   *
   * This is a server-side call so it prevents any data from being transfered
   * to the client.
   * @param get
   * @return true if the specified Get matches one or more keys, false if not
   * @throws IOException
   */
  public boolean exists(final Get get) throws IOException {
    return connection.getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, get.getRow()) {
        public Boolean call() throws IOException {
          return Boolean.valueOf(server.
            exists(location.getRegionInfo().getRegionName(), get));
        }
      }
    ).booleanValue();
  }

  /**
   * Commit to the table the buffer of BatchUpdate.
   * Called automatically in the commit methods when autoFlush is true.
   * @throws IOException
   */
  public void flushCommits() throws IOException {
    int last = 0;
    try {
      last = connection.processBatchOfRows(writeBuffer, tableName);
    } finally {
      writeBuffer.subList(0, last).clear();
      currentWriteBufferSize = 0;
      for (int i = 0; i < writeBuffer.size(); i++) {
        currentWriteBufferSize += writeBuffer.get(i).heapSize();
      }
    }
  }

  /**
   * Release held resources
   *
   * @throws IOException
  */
  public void close() throws IOException{
    flushCommits();
  }

  /**
   * Utility method that verifies Put is well formed.
   *
   * @param put
   * @throws IllegalArgumentException
   */
  private void validatePut(final Put put) throws IllegalArgumentException{
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<KeyValue> list : put.getFamilyMap().values()) {
        for (KeyValue kv : list) {
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  /**
   * Obtain a row lock
   * @param row The row to lock
   * @return rowLock RowLock containing row and lock id
   * @throws IOException
   */
  public RowLock lockRow(final byte [] row)
  throws IOException {
    return connection.getRegionServerWithRetries(
      new ServerCallable<RowLock>(connection, tableName, row) {
        public RowLock call() throws IOException {
          long lockId =
              server.lockRow(location.getRegionInfo().getRegionName(), row);
          RowLock rowLock = new RowLock(row,lockId);
          return rowLock;
        }
      }
    );
  }

  /**
   * Release a row lock
   * @param rl The row lock to release
   * @throws IOException
   */
  public void unlockRow(final RowLock rl)
  throws IOException {
    connection.getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, rl.getRow()) {
        public Boolean call() throws IOException {
          server.unlockRow(location.getRegionInfo().getRegionName(),
              rl.getLockId());
          return null; // FindBugs NP_BOOLEAN_RETURN_NULL
        }
      }
    );
  }

  /**
   * Get the value of autoFlush. If true, updates will not be buffered
   * @return value of autoFlush
   */
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * Set if this instanciation of HTable will autoFlush
   * @param autoFlush
   */
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  /**
   * Get the maximum size in bytes of the write buffer for this HTable
   * @return the size of the write buffer in bytes
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Set the size of the buffer in bytes.
   * If the new size is lower than the current size of data in the
   * write buffer, the buffer is flushed.
   * @param writeBufferSize
   * @throws IOException
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    if(currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * Get the write buffer
   * @return the current write buffer
   */
  public ArrayList<Put> getWriteBuffer() {
    return writeBuffer;
  }

  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner implements ResultScanner {
    private final Log CLIENT_LOG = LogFactory.getLog(this.getClass());
    // HEADSUP: The scan internal start row can change as we move through table.
    private Scan scan;
    private boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    private HRegionInfo currentRegion = null;
    private ScannerCallable callable = null;
    private final LinkedList<Result> cache = new LinkedList<Result>();
    private final int caching;
    private long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    private Result lastResult = null;

    protected ClientScanner(final Scan scan) {
      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Creating scanner over "
            + Bytes.toString(getTableName())
            + " starting at key '" + Bytes.toStringBinary(scan.getStartRow()) + "'");
      }
      this.scan = scan;
      this.lastNext = System.currentTimeMillis();

      // Use the caching from the Scan.  If not set, use the default cache setting for this table.
      if (this.scan.getCaching() > 0) {
        this.caching = this.scan.getCaching();
      } else {
        this.caching = HTable.this.scannerCaching;
      }

      // Removed filter validation.  We have a new format now, only one of all
      // the current filters has a validate() method.  We can add it back,
      // need to decide on what we're going to do re: filter redesign.
      // Need, at the least, to break up family from qualifier as separate
      // checks, I think it's important server-side filters are optimal in that
      // respect.
    }

    public void initialize() throws IOException {
      nextScanner(this.caching, false);
    }

    protected Scan getScan() {
      return scan;
    }

    protected long getTimestamp() {
      return lastNext;
    }

   /**
     * @param endKey
     * @return Returns true if the passed region endkey.
     */
    private boolean checkScanStopRow(final byte [] endKey) {
      if (this.scan.getStopRow().length > 0) {
        // there is a stop row, check to see if we are past it.
        byte [] stopRow = scan.getStopRow();
        int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
          endKey, 0, endKey.length);
        if (cmp <= 0) {
          // stopRow <= endKey (endKey is equals to or larger than stopRow)
          // This is a stop.
          return true;
        }
      }
      return false; //unlikely.
    }

    /*
     * Gets a scanner for the next region.  If this.currentRegion != null, then
     * we will move to the endrow of this.currentRegion.  Else we will get
     * scanner at the scan.getStartRow().  We will go no further, just tidy
     * up outstanding scanners, if <code>currentRegion != null</code> and
     * <code>done</code> is true.
     * @param nbRows
     * @param done Server-side says we're done scanning.
     */
    private boolean nextScanner(int nbRows, final boolean done)
    throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        getConnection().getRegionServerWithRetries(callable);
        this.callable = null;
      }

      // Where to start the next scanner
      byte [] localStartKey = null;

      // if we're at end of table, close and return false to stop iterating
      if (this.currentRegion != null) {
        byte [] endKey = this.currentRegion.getEndKey();
        if (endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
            checkScanStopRow(endKey) ||
            done) {
          close();
          if (CLIENT_LOG.isDebugEnabled()) {
            CLIENT_LOG.debug("Finished with scanning at " + this.currentRegion);
          }
          return false;
        }
        localStartKey = endKey;
        if (CLIENT_LOG.isDebugEnabled()) {
          CLIENT_LOG.debug("Finished with region " + this.currentRegion);
        }
      } else {
        localStartKey = this.scan.getStartRow();
      }

      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
      }
      try {
        callable = getScannerCallable(localStartKey, nbRows);
        // Open a scanner on the region server starting at the
        // beginning of the region
        getConnection().getRegionServerWithRetries(callable);
        this.currentRegion = callable.getHRegionInfo();
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }

    protected ScannerCallable getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s = new ScannerCallable(getConnection(),
        getTableName(), scan);
      s.setCaching(nbRows);
      return s;
    }

    public Result next() throws IOException {
      // If the scanner is closed but there is some rows left in the cache,
      // it will first empty it before returning null
      if (cache.size() == 0 && this.closed) {
        return null;
      }
      if (cache.size() == 0) {
        Result [] values = null;
        long remainingResultSize = maxScannerResultSize;
        int countdown = this.caching;
        // We need to reset it if it's a new callable that was created
        // with a countdown in nextScanner
        callable.setCaching(this.caching);
        // This flag is set when we want to skip the result returned.  We do
        // this when we reset scanner because it split under us.
        boolean skipFirst = false;
        do {
          try {
            // Server returns a null values if scanning is to stop.  Else,
            // returns an empty array if scanning is to go on and we've just
            // exhausted current region.
            values = getConnection().getRegionServerWithRetries(callable);
            if (skipFirst) {
              skipFirst = false;
              // Reget.
              values = getConnection().getRegionServerWithRetries(callable);
            }
          } catch (DoNotRetryIOException e) {
            Throwable cause = e.getCause();
            if (cause == null || !(cause instanceof NotServingRegionException)) {
              throw e;
            }
            // Else, its signal from depths of ScannerCallable that we got an
            // NSRE on a next and that we need to reset the scanner.
            if (this.lastResult != null) {
              this.scan.setStartRow(this.lastResult.getRow());
              // Skip first row returned.  We already let it out on previous
              // invocation.
              skipFirst = true;
            }
            // Clear region
            this.currentRegion = null;
            continue;
          } catch (IOException e) {
            if (e instanceof UnknownScannerException &&
                lastNext + scannerTimeout < System.currentTimeMillis()) {
              ScannerTimeoutException ex = new ScannerTimeoutException();
              ex.initCause(e);
              throw ex;
            }
            throw e;
          }
          lastNext = System.currentTimeMillis();
          if (values != null && values.length > 0) {
            for (Result rs : values) {
              cache.add(rs);
              for (KeyValue kv : rs.raw()) {
                  remainingResultSize -= kv.heapSize();
              }
              countdown--;
              this.lastResult = rs;
            }
          }
          // Values == null means server-side filter has determined we must STOP
        } while (remainingResultSize > 0 && countdown > 0 && nextScanner(countdown, values == null));
      }

      if (cache.size() > 0) {
        return cache.poll();
      }
      return null;
    }

    /**
     * Get <param>nbRows</param> rows.
     * How many RPCs are made is determined by the {@link Scan#setCaching(int)}
     * setting (or hbase.client.scanner.caching in hbase-site.xml).
     * @param nbRows number of rows to return
     * @return Between zero and <param>nbRows</param> RowResults.  Scan is done
     * if returned array is of zero-length (We never return null).
     * @throws IOException
     */
    public Result [] next(int nbRows) throws IOException {
      // Collect values to be returned here
      ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
      for(int i = 0; i < nbRows; i++) {
        Result next = next();
        if (next != null) {
          resultSets.add(next);
        } else {
          break;
        }
      }
      return resultSets.toArray(new Result[resultSets.size()]);
    }

    public void close() {
      if (callable != null) {
        callable.setClose();
        try {
          getConnection().getRegionServerWithRetries(callable);
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        }
        callable = null;
      }
      closed = true;
    }

    public Iterator<Result> iterator() {
      return new Iterator<Result>() {
        // The next RowResult, possibly pre-read
        Result next = null;

        // return true if there is another item pending, false if there isn't.
        // this method is where the actual advancing takes place, but you need
        // to call next() to consume it. hasNext() will only advance if there
        // isn't a pending next().
        public boolean hasNext() {
          if (next == null) {
            try {
              next = ClientScanner.this.next();
              return next != null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return true;
        }

        // get the pending next item and advance the iterator. returns null if
        // there is no next item.
        public Result next() {
          // since hasNext() does the real advancing, we call this to determine
          // if there is a next before proceeding.
          if (!hasNext()) {
            return null;
          }

          // if we get to here, then hasNext() has given us an item to return.
          // we want to return the item and then null out the next pointer, so
          // we use a temporary variable.
          Result temp = next;
          next = null;
          return temp;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
