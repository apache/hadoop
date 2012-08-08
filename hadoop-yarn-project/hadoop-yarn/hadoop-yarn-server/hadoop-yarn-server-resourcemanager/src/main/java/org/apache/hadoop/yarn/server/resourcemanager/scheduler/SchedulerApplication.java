package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * Represents an Application from the viewpoint of the scheduler.
 * Each running Application in the RM corresponds to one instance
 * of this class.
 */
@Private
@Unstable
public abstract class SchedulerApplication {

  /**
   * Get {@link ApplicationAttemptId} of the application master.
   * @return <code>ApplicationAttemptId</code> of the application master
   */
  public abstract ApplicationAttemptId getApplicationAttemptId();
  
  /**
   * Get the live containers of the application.
   * @return live containers of the application
   */
  public abstract Collection<RMContainer> getLiveContainers();
  
  /**
   * Get the reserved containers of the application.
   * @return the reserved containers of the application
   */
  public abstract Collection<RMContainer> getReservedContainers();
  
  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public abstract boolean isPending();

}
