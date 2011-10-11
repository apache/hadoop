package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

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
  
  /** Stop the services when exiting standby state */
  public void stopStandbyServices() throws IOException;
}
