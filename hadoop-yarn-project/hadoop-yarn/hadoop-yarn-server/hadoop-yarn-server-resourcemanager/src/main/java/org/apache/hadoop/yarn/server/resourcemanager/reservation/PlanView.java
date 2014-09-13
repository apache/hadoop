package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * This interface provides a read-only view on the allocations made in this
 * plan. This methods are used for example by {@link ReservationAgent}s to
 * determine the free resources in a certain point in time, and by
 * PlanFollowerPolicy to publish this plan to the scheduler.
 */
public interface PlanView extends PlanContext {

  /**
   * Return a {@link ReservationAllocation} identified by its
   * {@link ReservationId}
   * 
   * @param reservationID the unique id to identify the
   *          {@link ReservationAllocation}
   * @return {@link ReservationAllocation} identified by the specified id
   */
  public ReservationAllocation getReservationById(ReservationId reservationID);

  /**
   * Gets all the active reservations at the specified point of time
   * 
   * @param tick the time (UTC in ms) for which the active reservations are
   *          requested
   * @return set of active reservations at the specified time
   */
  public Set<ReservationAllocation> getReservationsAtTime(long tick);

  /**
   * Gets all the reservations in the plan
   * 
   * @return set of all reservations handled by this Plan
   */
  public Set<ReservationAllocation> getAllReservations();

  /**
   * Returns the total {@link Resource} reserved for all users at the specified
   * time
   * 
   * @param tick the time (UTC in ms) for which the reserved resources are
   *          requested
   * @return the total {@link Resource} reserved for all users at the specified
   *         time
   */
  public Resource getTotalCommittedResources(long tick);

  /**
   * Returns the total {@link Resource} reserved for a given user at the
   * specified time
   * 
   * @param user the user who made the reservation(s)
   * @param tick the time (UTC in ms) for which the reserved resources are
   *          requested
   * @return the total {@link Resource} reserved for a given user at the
   *         specified time
   */
  public Resource getConsumptionForUser(String user, long tick);

  /**
   * Returns the overall capacity in terms of {@link Resource} assigned to this
   * plan (typically will correspond to the absolute capacity of the
   * corresponding queue).
   * 
   * @return the overall capacity in terms of {@link Resource} assigned to this
   *         plan
   */
  public Resource getTotalCapacity();

  /**
   * Gets the time (UTC in ms) at which the first reservation starts
   * 
   * @return the time (UTC in ms) at which the first reservation starts
   */
  public long getEarliestStartTime();

  /**
   * Returns the time (UTC in ms) at which the last reservation terminates
   * 
   * @return the time (UTC in ms) at which the last reservation terminates
   */
  public long getLastEndTime();

}
