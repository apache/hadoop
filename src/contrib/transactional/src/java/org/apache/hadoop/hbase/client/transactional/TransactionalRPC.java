package org.apache.hadoop.hbase.client.transactional;

import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;

/** Simple class for registering the transactional RPC codes. 
 *  
 */
public final class TransactionalRPC {
  
  private static final byte RPC_CODE = 100;

  private static boolean initialized = false;
  
  public synchronized static void initialize() {
    if (initialized) {
      return;
    }
    HBaseRPC.addToMap(TransactionalRegionInterface.class, RPC_CODE);
    initialized = true;
  }
  
  private TransactionalRPC() {
    // Static helper class;
  }

}
