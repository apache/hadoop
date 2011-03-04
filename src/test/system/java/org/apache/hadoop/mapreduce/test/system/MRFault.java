package org.apache.hadoop.mapreduce.test.system;

/**
 * Fault injection types. At a given time any of these faults (0 or more) 
 * can be injected. 
 * @see AbstractMasterSlaveCluster#enable(List<Enum>)
 * @see AbstractMasterSlaveCluster#disable(List<Enum>)
 */
public enum MRFault {
  BAD_NODE_HEALTH,
  STALL_HEARTBEAT
}
