package org.apache.hadoop.yarn.server.resourcemanager.reservation;

/**
 * A Plan represents the central data structure of a reservation system that
 * maintains the "agenda" for the cluster. In particular, it maintains
 * information on how a set of {@link ReservationDefinition} that have been
 * previously accepted will be honored.
 * 
 * {@link ReservationDefinition} submitted by the users through the RM public
 * APIs are passed to appropriate {@link ReservationAgent}s, which in turn will
 * consult the Plan (via the {@link PlanView} interface) and try to determine
 * whether there are sufficient resources available in this Plan to satisfy the
 * temporal and resource constraints of a {@link ReservationDefinition}. If a
 * valid allocation is found the agent will try to store it in the plan (via the
 * {@link PlanEdit} interface). Upon success the system return to the user a
 * positive acknowledgment, and a reservation identifier to be later used to
 * access the reserved resources.
 * 
 * A {@link PlanFollower} will continuously read from the Plan and will
 * affect the instantaneous allocation of resources among jobs running by
 * publishing the "current" slice of the Plan to the underlying scheduler. I.e.,
 * the configuration of queues/weights of the scheduler are modified to reflect
 * the allocations in the Plan.
 * 
 * As this interface have several methods we decompose them into three groups:
 * {@link PlanContext}: containing configuration type information,
 * {@link PlanView} read-only access to the plan state, and {@link PlanEdit}
 * write access to the plan state.
 */
public interface Plan extends PlanContext, PlanView, PlanEdit {

}
