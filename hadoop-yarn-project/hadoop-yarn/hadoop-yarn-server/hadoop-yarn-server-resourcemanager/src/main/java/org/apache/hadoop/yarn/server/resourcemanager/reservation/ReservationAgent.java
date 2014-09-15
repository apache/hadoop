package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * An entity that seeks to acquire resources to satisfy an user's contract
 */
public interface ReservationAgent {

  /**
   * Create a reservation for the user that abides by the specified contract
   * 
   * @param reservationId the identifier of the reservation to be created.
   * @param user the user who wants to create the reservation
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources the user requires for his
   *          session
   * 
   * @return whether the create operation was successful or not
   * @throws PlanningException if the session cannot be fitted into the plan
   */
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  /**
   * Update a reservation for the user that abides by the specified contract
   * 
   * @param reservationId the identifier of the reservation to be updated
   * @param user the user who wants to create the session
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources the user requires for his
   *          reservation
   * 
   * @return whether the update operation was successful or not
   * @throws PlanningException if the reservation cannot be fitted into the plan
   */
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  /**
   * Delete an user reservation
   * 
   * @param reservationId the identifier of the reservation to be deleted
   * @param user the user who wants to create the reservation
   * @param plan the Plan to which the session must be fitted
   * 
   * @return whether the delete operation was successful or not
   * @throws PlanningException if the reservation cannot be fitted into the plan
   */
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException;

}
