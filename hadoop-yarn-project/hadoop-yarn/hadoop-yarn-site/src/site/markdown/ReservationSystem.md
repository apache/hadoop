<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Reservation System
==================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------

This document provides a brief overview of the `YARN ReservationSystem`.

Overview
--------

The `ReservationSystem` of YARN provides the user the ability to reserve resources over (and ahead of) time, to ensure that important production jobs will be run very predictably. The ReservationSystem performs careful admission control and provides guarantees over absolute amounts of resources (instead of % of cluster size). Reservation can be both malleable or have gang semantics, and can have time-varying resource requirements. The ReservationSystem is a component of the YARN ResourceManager.


Flow of a Reservation
----------------------

![YARN Reservation System | width=600px](./images/yarn_reservation_system.png)

With reference to the figure above, a typical reservation proceeds as follows:

 * **Step 0**  The user (or an automated tool on its behalf) submits a reservation creation request, and receives a response containing the ReservationId.

 * **Step 1**  The user (or an automated tool on its behalf) submits a reservation request specified by the Reservation Definition Language (RDL) and ReservationId retrieved from the previous step. This describes the user need for resources over-time (e.g., a skyline of resources) and temporal constraints (e.g., deadline). This can be done both programmatically through the usual Client-to-RM protocols or via the REST api of the RM. If a reservation is submitted with the same ReservationId, and the RDL is the same, a new reservation will not be created and the request will be successful. If the RDL is different, the reservation will be rejected, and the request will be unsuccessful.

 * **Step 2**  The ReservationSystem leverages a ReservationAgent (GREE in the figure) to find a plausible allocation for the reservation in the Plan, a data structure tracking all reservation currently accepted and the available resources in the system.

 * **Step 3**  The SharingPolicy provides a way to enforce invariants on the reservation being accepted, potentially rejecting reservations. For example, the CapacityOvertimePolicy allows enforcement of both instantaneous max-capacity a user can request across all of his/her reservations and a limit on the integral of resources over a period of time, e.g., the user can reserve up to 50% of the cluster capacity instantanesouly, but in any 24h period of time he/she cannot exceed 10% average.

 * **Step 4**  Upon a successful validation the ReservationSystem returns to the user a ReservationId (think of it as an airline ticket).

 * **Step 5**  When the time comes, a new component called the PlanFollower publishes the state of the plan to the scheduler, by dynamically creating/tweaking/destroying queues.

 * **Step 6**  The user can then submit one (or more) jobs to the reservable queue, by simply including the ReservationId as part of the ApplicationSubmissionContext.

 * **Step 7**  The Scheduler will then provide containers from a special queue created to ensure resources reservation is respected. Within the limits of the reservation, the user has guaranteed access to the resources, above that resource sharing proceed with standard Capacity/Fairness sharing.

 * **Step 8**  The system includes mechanisms to adapt to drop in cluster capacity. This consists in replanning by "moving" the reservation if possible, or rejecting the smallest amount of previously accepted reservation (to ensure that other reservation will receive their full amount).





Configuring the Reservation System
----------------------------------

Configuring the `ReservationSystem` is simple. Currently we have added support for *reservations* in both `CapacityScheduler` and `FairScheduler`. You can mark any **leaf queue** in the **capacity-scheduler.xml** or **fair-scheduler.xml** as available for "reservations" (see [CapacityScheduler](./CapacityScheduler.html#Configuring_ReservationSystem_with_CapacityScheduler) and the [FairScheduler](./FairScheduler.html#Configuring_ReservationSystem) for details). Then the capacity/fair share within that queue can be used for making reservations. Jobs can still be submitted to the *reservable queue* without a reservation, in which case they will be run in best-effort mode in whatever capacity is left over by the jobs running within active reservations.
