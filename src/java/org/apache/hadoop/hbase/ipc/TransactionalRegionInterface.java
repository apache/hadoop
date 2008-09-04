/*
 * $Id$
 * Created on Jun 4, 2008
 * 
 */
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;

/**
 * Interface for transactional region servers.
 * 
 */
public interface TransactionalRegionInterface extends HRegionInterface {
  /** Interface version number */
  public static final long versionID = 1L;

  /**
   * Sent to initiate a transaction.
   * 
   * @param transactionId
   * @param regionName name of region
   */
  public void beginTransaction(long transactionId, final byte[] regionName)
      throws IOException;

  /**
   * Retrieve a single value from the specified region for the specified row and
   * column keys
   * 
   * @param regionName name of region
   * @param row row key
   * @param column column key
   * @return alue for that region/row/column
   * @throws IOException
   */
  public Cell get(long transactionId, final byte[] regionName,
      final byte[] row, final byte[] column) throws IOException;

  /**
   * Get the specified number of versions of the specified row and column
   * 
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param numVersions number of versions to return
   * @return array of values
   * @throws IOException
   */
  public Cell[] get(long transactionId, final byte[] regionName,
      final byte[] row, final byte[] column, final int numVersions)
      throws IOException;

  /**
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param timestamp timestamp
   * @param numVersions number of versions to return
   * @return array of values
   * @throws IOException
   */
  public Cell[] get(long transactionId, final byte[] regionName,
      final byte[] row, final byte[] column, final long timestamp,
      final int numVersions) throws IOException;

  /**
   * Get all the data for the specified row at a given timestamp
   * 
   * @param regionName region name
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  public RowResult getRow(long transactionId, final byte[] regionName,
      final byte[] row, final long ts) throws IOException;

  /**
   * Get selected columns for the specified row at a given timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  public RowResult getRow(long transactionId, final byte[] regionName,
      final byte[] row, final byte[][] columns, final long ts)
      throws IOException;

  /**
   * Get selected columns for the specified row at the latest timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  public RowResult getRow(long transactionId, final byte[] regionName,
      final byte[] row, final byte[][] columns) throws IOException;

  /**
   * Delete all cells that match the passed row and whose timestamp is equal-to
   * or older than the passed timestamp.
   * 
   * @param regionName region name
   * @param row row key
   * @param timestamp Delete all entries that have this timestamp or older
   * @throws IOException
   */
  public void deleteAll(long transactionId, byte[] regionName, byte[] row,
      long timestamp) throws IOException;

  /**
   * Opens a remote scanner with a RowFilter.
   * 
   * @param transactionId
   * @param regionName name of region to scan
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned. Its also possible to
   * pass a regex for column family name. A column name is judged to be regex if
   * it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row to scan
   * @param timestamp only return values whose timestamp is <= this value
   * @param filter RowFilter for filtering results at the row-level.
   * 
   * @return scannerId scanner identifier used in other calls
   * @throws IOException
   */
  public long openScanner(final long transactionId, final byte[] regionName,
      final byte[][] columns, final byte[] startRow, long timestamp,
      RowFilterInterface filter) throws IOException;

  /**
   * Applies a batch of updates via one RPC
   * 
   * @param regionName name of the region to update
   * @param b BatchUpdate
   * @throws IOException
   */
  public void batchUpdate(long transactionId, final byte[] regionName,
      final BatchUpdate b) throws IOException;

  /**
   * Ask if we can commit the given transaction.
   * 
   * @param transactionId
   * @return true if we can commit
   */
  public boolean commitRequest(final byte[] regionName, long transactionId)
      throws IOException;

  /**
   * Commit the transaction.
   * 
   * @param transactionId
   */
  public void commit(final byte[] regionName, long transactionId)
      throws IOException;

  /**
   * Abort the transaction.
   * 
   * @param transactionId
   */
  public void abort(final byte[] regionName, long transactionId)
      throws IOException;
}
