package org.apache.hadoop.dfs;

import java.io.IOException;


/**
 * This exception is thrown when a datanode tries to register or communicate
 * with the namenode when it does not appear on the list of included nodes, 
 * or has been specifically excluded.
 * 
 * @author Wendy Chien
 */
class DisallowedDatanodeException extends IOException {

  public DisallowedDatanodeException( DatanodeID nodeID ) {
    super("Datanode denied communication with namenode: " + nodeID.getName() );
  }
}
