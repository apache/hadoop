package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class TestMinimumServerCount extends HBaseClusterTestCase {

  static final String TABLE_NAME = "TestTable";

  public TestMinimumServerCount() {
    // start cluster with one region server only
    super(1, true);
  }

  boolean isTableAvailable(String tableName) throws IOException {
    boolean available = true;
    HTable meta = new HTable(conf, ".META.");
    ResultScanner scanner = meta.getScanner(HConstants.CATALOG_FAMILY);
    Result result;
    while ((result = scanner.next()) != null) {
      // set available to false if a region of the table is found with no
      // assigned server
      byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
        HConstants.SERVER_QUALIFIER);
      if (value == null) {
        available = false;
        break;
      }
    }
    return available;
  }

  public void testMinimumServerCount() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);

    // create and disable table
    admin.createTable(createTableDescriptor(TABLE_NAME));
    admin.disableTable(TABLE_NAME);
    assertFalse(admin.isTableEnabled(TABLE_NAME));

    // reach in and set minimum server count
    cluster.hbaseCluster.getMaster().getServerManager()
      .setMinimumServerCount(2);

    // now try to enable the table
    try {
      admin.enableTable(TABLE_NAME);
    } catch (IOException ex) {
      // ignore
    }
    Thread.sleep(10 * 1000);
    assertFalse(admin.isTableAvailable(TABLE_NAME));
    
    // now start another region server
    cluster.startRegionServer();

    // sleep a bit for assignment
    Thread.sleep(10 * 1000);
    assertTrue(admin.isTableAvailable(TABLE_NAME));
  }

}
