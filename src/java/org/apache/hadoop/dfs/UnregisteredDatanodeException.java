package org.apache.hadoop.dfs;

import java.io.IOException;


/**
 * This exception is thrown when a datanode that has not previously 
 * registered is trying to access the name node.
 * 
 */
class UnregisteredDatanodeException extends IOException {

  public UnregisteredDatanodeException(DatanodeID nodeID) {
    super("Unregistered data node: " + nodeID.getName());
  }

  public UnregisteredDatanodeException(DatanodeID nodeID, 
                                       DatanodeInfo storedNode) {
    super("Data node " + nodeID.getName() 
          + " is attempting to report storage ID "
          + nodeID.getStorageID() + ". Node " 
          + storedNode.getName() + " is expected to serve this storage.");
  }
}
