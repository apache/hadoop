package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * This exception indicate that the reservation that has been attempted, would
 * exceed the physical resources available in the {@link Plan} at the moment.
 */
@Public
@Unstable
public class ResourceOverCommitException extends PlanningException {

  private static final long serialVersionUID = 7070699407526521032L;

  public ResourceOverCommitException(String message) {
    super(message);
  }

  public ResourceOverCommitException(Throwable cause) {
    super(cause);
  }

  public ResourceOverCommitException(String message, Throwable cause) {
    super(message, cause);
  }

}
