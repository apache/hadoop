package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Scanner class that contains the <code>.META.</code> table scanning logic 
 * and uses a Retryable scanner. Provided visitors will be called
 * for each row.
 */
class MetaScanner implements HConstants {

  /**
   * Scans the meta table and calls a visitor on each RowResult and uses a empty
   * start row value as table name.
   * 
   * @param configuration
   * @param visitor A custom visitor
   * @throws IOException
   */
  public static void metaScan(HBaseConfiguration configuration,
      MetaScannerVisitor visitor)
  throws IOException {
    metaScan(configuration, visitor, EMPTY_START_ROW);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name to locate meta regions.
   * 
   * @param configuration
   * @param visitor
   * @param tableName
   * @throws IOException
   */
  public static void metaScan(HBaseConfiguration configuration,
      MetaScannerVisitor visitor, byte[] tableName)
  throws IOException {
    HConnection connection = HConnectionManager.getConnection(configuration);
    boolean toContinue = true;
    byte [] startRow = Bytes.equals(tableName, EMPTY_START_ROW)? tableName:
      HRegionInfo.createRegionName(tableName, null, NINES);
      
    // Scan over each meta region
    do {
      ScannerCallable callable = new ScannerCallable(connection,
        META_TABLE_NAME, COL_REGIONINFO_ARRAY, tableName, LATEST_TIMESTAMP,
        null);
      try {
        // Open scanner
        connection.getRegionServerWithRetries(callable);
        while (toContinue) {
          RowResult rowResult = connection.getRegionServerWithRetries(callable);
          if (rowResult == null || rowResult.size() == 0) {
            break;
          }
          HRegionInfo info = Writables.getHRegionInfo(rowResult
              .get(COL_REGIONINFO));
          List<byte []> parse = HRegionInfo.parseMetaRegionRow(info.getRegionName());
          HRegionLocation regionLocation =
            connection.locateRegion(parse.get(0), parse.get(1));
          toContinue = visitor.processRow(rowResult, regionLocation, info);
        }
        // Advance the startRow to the end key of the current region
        startRow = callable.getHRegionInfo().getEndKey();
      } finally {
        // Close scanner
        callable.setClose();
        connection.getRegionServerWithRetries(callable);
      }
    } while (Bytes.compareTo(startRow, LAST_ROW) != 0);
  }

  /**
   * Visitor class called to process each row of the .META. table
   */
  protected interface MetaScannerVisitor {

    /**
     * Visitor method that accepts a RowResult and the meta region location.
     * Implementations can return false to stop the region's loop if it becomes
     * unnecessary for some reason.
     * 
     * @param rowResult
     * @param regionLocation
     * @param info
     * @return A boolean to know if it should continue to loop in the region
     * @throws IOException
     */
    public boolean processRow(RowResult rowResult,
        HRegionLocation regionLocation, HRegionInfo info) throws IOException;
  }
}
