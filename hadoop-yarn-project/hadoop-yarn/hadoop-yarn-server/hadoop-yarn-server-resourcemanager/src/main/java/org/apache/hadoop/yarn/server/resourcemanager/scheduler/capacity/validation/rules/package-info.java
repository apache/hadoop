/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Capacity Scheduler configuration validation rules.
 * <p>
 * There is one rule per shared check home, and each rule calls the shared
 * check functions or the throwing getters that queue construction uses, so
 * every check has a single implementation. A rule reports ERROR only where a
 * refresh rejects the configuration; checks that do not exist on the refresh
 * path are WARNING.
 * <p>
 * <table>
 * <caption>Rule ids</caption>
 * <tr><th>Rule</th><th>Rule id</th><th>Severity</th></tr>
 * <tr><td>CapacityRule</td><td>invalid-capacity,
 * invalid-capacity-resource, invalid-capacity-vector,
 * maximum-resource-exceeds-parent, minimum-resource-exceeds-maximum,
 * mixed-capacity-config-type, mixed-children-capacity-types,
 * absolute-capacity-mismatch, children-minimum-resource-exceeds-parent,
 * children-capacity-sum, mixed-dynamic-children-capacity-types</td>
 * <td>ERROR</td></tr>
 * <tr><td>NodeLabelRule</td><td>invalid-node-label-key,
 * child-labels-not-subset, invalid-default-label-expression</td>
 * <td>ERROR</td></tr>
 * <tr><td>NodeLabelRule</td><td>root-accessible-labels-ignored</td>
 * <td>WARNING</td></tr>
 * <tr><td>NodeLabelRule</td><td>invalid-template-label</td>
 * <td>ERROR, WARNING when the parent accesses any label</td></tr>
 * <tr><td>StateRule</td><td>invalid-state,
 * running-queue-under-stopped-parent, parent-queue-not-running</td>
 * <td>ERROR</td></tr>
 * <tr><td>HierarchyRule</td><td>queue-removal-not-stopped,
 * leaf-to-parent-conversion, managed-parent-conversion</td>
 * <td>ERROR</td></tr>
 * <tr><td>StructureRule</td><td>missing-child-queues,
 * reservable-parent-queue, managed-parent-template-type</td>
 * <td>ERROR</td></tr>
 * <tr><td>AllocationRule</td><td>invalid-memory-allocation,
 * invalid-vcores-allocation, invalid-maximum-allocation,
 * maximum-allocation-exceeds-cluster, maximum-allocation-decreased</td>
 * <td>ERROR</td></tr>
 * <tr><td>LimitRule</td><td>invalid-user-weight, user-weight-out-of-range,
 * default-lifetime-exceeds-maximum</td><td>ERROR</td></tr>
 * <tr><td>ValueRule</td><td>invalid-priority-acl, invalid-ordering-policy,
 * invalid-queue-ordering-policy, invalid-multi-node-policy,
 * invalid-queue-management-policy</td><td>ERROR</td></tr>
 * <tr><td>ValueRule</td><td>offswitch-assignments-below-one</td>
 * <td>WARNING</td></tr>
 * <tr><td>ValueRule</td><td>invalid-value</td><td>ERROR, WARNING for values
 * read only at dynamic queue creation or by the reservation system</td></tr>
 * <tr><td>PlacementRulesRule</td><td>duplicate-placement-rules,
 * invalid-placement-rule, invalid-mapping-rules,
 * invalid-mapping-rule-target, invalid-workflow-priority-mapping</td>
 * <td>ERROR</td></tr>
 * <tr><td>PlacementRulesRule</td><td>mapping-rule-json-missing</td>
 * <td>WARNING</td></tr>
 * <tr><td>the validate/v2 endpoint</td><td>invalid-mutation, a mutation
 * that cannot be applied, such as removing a queue that does not exist</td>
 * <td>ERROR</td></tr>
 * </table>
 * <p>
 * Not evaluated: checks that need effective resources or live entitlements,
 * such as the capacity calculation warnings of non-legacy queue mode, checks
 * that run only at startup, during application recovery or for dynamic queue
 * operations, and the mutation ACLs of the caller.
 * <p>
 * Checks that do not exist on the refresh path, all WARNING:
 * capacity-exceeds-maximum-capacity and maximum-capacity-out-of-range
 * (CapacityRule), value-out-of-range (LimitRule), unknown-node-label
 * (NodeLabelRule), duplicate-child-queue and conflicting-auto-queue-creation
 * (StructureRule), queue-removal-with-applications (HierarchyRule).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation.rules;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
