package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.StandbyException;

/**
 * Context that is to be used by {@link HAState} for getting/setting the
 * current state and performing required operations.
 */
@InterfaceAudience.Private
public interface HAContext {
  /** Set the state of the context to given {@code state} */
  public void setState(HAState state);
  
  /** Get the state from the context */
  public HAState getState();
  
  /** Start the services required in active state */
  public void startActiveServices() throws IOException;
  
  /** Stop the services when exiting active state */
  public void stopActiveServices() throws IOException;
  
  /** Start the services required in standby state */
  public void startStandbyServices() throws IOException;

  /** Prepare to exit the standby state */
  public void prepareToStopStandbyServices() throws ServiceFailedException;

  /** Stop the services when exiting standby state */
  public void stopStandbyServices() throws IOException;

  /**
   * Take a write-lock on the underlying namesystem
   * so that no concurrent state transitions or edits
   * can be made.
   */
  void writeLock();

  /**
   * Unlock the lock taken by {@link #writeLock()}
   */
  void writeUnlock();

  /**
   * Verify that the given operation category is allowed in the
   * current state. This is to allow NN implementations (eg BackupNode)
   * to override it with node-specific handling.
   */
  void checkOperation(OperationCategory op) throws StandbyException;

  /**
   * @return true if the node should allow stale reads (ie reads
   * while the namespace is not up to date)
   */
  boolean allowStaleReads();
}
