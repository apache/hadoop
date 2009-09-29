package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

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
    byte [] startRow = tableName == null || tableName.length == 0 ?
        HConstants.EMPTY_START_ROW : 
          HRegionInfo.createRegionName(tableName, null, ZEROES);
      
    // Scan over each meta region
    ScannerCallable callable = null;
    int rows = configuration.getInt("hbase.meta.scanner.caching", 100); 
    do {
      Scan scan = new Scan(startRow).addFamily(CATALOG_FAMILY);
      callable = new ScannerCallable(connection, META_TABLE_NAME, scan);
      // Open scanner
      connection.getRegionServerWithRetries(callable);
      try {
        callable.setCaching(rows);
        done: do {
          //we have all the rows here 
          Result [] rrs = connection.getRegionServerWithRetries(callable);
          if (rrs == null || rrs.length == 0 || rrs[0].size() == 0) {
            break done; //exit completely
          }
          for (int i = 0; i < rrs.length; i++) {
            if (!visitor.processRow(rrs[i]))
              break done; //exit completely
          }
          //here, we didn't break anywhere. Check if we have more rows
        } while(true);
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
  interface MetaScannerVisitor {
    /**
     * Visitor method that accepts a RowResult and the meta region location.
     * Implementations can return false to stop the region's loop if it becomes
     * unnecessary for some reason.
     * 
     * @param rowResult
     * @return A boolean to know if it should continue to loop in the region
     * @throws IOException
     */
    public boolean processRow(Result rowResult) throws IOException;
  }
}
