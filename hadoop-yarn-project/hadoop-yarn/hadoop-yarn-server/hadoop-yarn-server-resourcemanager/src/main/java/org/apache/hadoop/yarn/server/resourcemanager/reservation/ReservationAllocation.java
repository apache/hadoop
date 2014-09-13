package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * A ReservationAllocation represents a concrete allocation of resources over
 * time that satisfy a certain {@link ReservationDefinition}. This is used
 * internally by a {@link Plan} to store information about how each of the
 * accepted {@link ReservationDefinition} have been allocated.
 */
public interface ReservationAllocation extends
    Comparable<ReservationAllocation> {

  /**
   * Returns the unique identifier {@link ReservationId} that represents the
   * reservation
   * 
   * @return reservationId the unique identifier {@link ReservationId} that
   *         represents the reservation
   */
  public ReservationId getReservationId();

  /**
   * Returns the original {@link ReservationDefinition} submitted by the client
   * 
   * @return
   */
  public ReservationDefinition getReservationDefinition();

  /**
   * Returns the time at which the reservation is activated
   * 
   * @return the time at which the reservation is activated
   */
  public long getStartTime();

  /**
   * Returns the time at which the reservation terminates
   * 
   * @return the time at which the reservation terminates
   */
  public long getEndTime();

  /**
   * Returns the map of resources requested against the time interval for which
   * they were
   * 
   * @return the allocationRequests the map of resources requested against the
   *         time interval for which they were
   */
  public Map<ReservationInterval, ReservationRequest> getAllocationRequests();

  /**
   * Return a string identifying the plan to which the reservation belongs
   * 
   * @return the plan to which the reservation belongs
   */
  public String getPlanName();

  /**
   * Returns the user who requested the reservation
   * 
   * @return the user who requested the reservation
   */
  public String getUser();

  /**
   * Returns whether the reservation has gang semantics or not
   * 
   * @return true if there is a gang request, false otherwise
   */
  public boolean containsGangs();

  /**
   * Sets the time at which the reservation was accepted by the system
   * 
   * @param acceptedAt the time at which the reservation was accepted by the
   *          system
   */
  public void setAcceptanceTimestamp(long acceptedAt);

  /**
   * Returns the time at which the reservation was accepted by the system
   * 
   * @return the time at which the reservation was accepted by the system
   */
  public long getAcceptanceTime();

  /**
   * Returns the capacity represented by cumulative resources reserved by the
   * reservation at the specified point of time
   * 
   * @param tick the time (UTC in ms) for which the reserved resources are
   *          requested
   * @return the resources reserved at the specified time
   */
  public Resource getResourcesAtTime(long tick);

}
