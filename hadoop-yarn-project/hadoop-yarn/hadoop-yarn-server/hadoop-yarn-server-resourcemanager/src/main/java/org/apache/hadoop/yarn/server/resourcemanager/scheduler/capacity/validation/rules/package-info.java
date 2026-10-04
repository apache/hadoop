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
 * Check ids are those of the configuration check inventory.
 * <table>
 * <caption>Check id to rule id</caption>
 * <tr><th>Check</th><th>Rule</th><th>Rule id</th><th>Severity</th></tr>
 * <tr><td>C01 to C04</td><td>CapacityRule</td><td>invalid-capacity,
 * invalid-capacity-resource, invalid-capacity-vector</td><td>ERROR</td></tr>
 * <tr><td>C05</td><td>none, the root capacity getter always returns 100
 * </td><td></td><td></td></tr>
 * <tr><td>C06</td><td>CapacityRule</td>
 * <td>maximum-resource-exceeds-parent</td><td>ERROR</td></tr>
 * <tr><td>C07</td><td>CapacityRule</td>
 * <td>minimum-resource-exceeds-maximum</td><td>ERROR</td></tr>
 * <tr><td>C08</td><td>CapacityRule</td><td>mixed-capacity-config-type</td>
 * <td>ERROR</td></tr>
 * <tr><td>C09</td><td>CapacityRule</td><td>mixed-children-capacity-types</td>
 * <td>ERROR</td></tr>
 * <tr><td>C10</td><td>CapacityRule</td><td>absolute-capacity-mismatch</td>
 * <td>ERROR</td></tr>
 * <tr><td>C11</td><td>CapacityRule</td>
 * <td>children-minimum-resource-exceeds-parent</td><td>ERROR</td></tr>
 * <tr><td>C12 to C14</td><td>CapacityRule</td><td>children-capacity-sum</td>
 * <td>ERROR</td></tr>
 * <tr><td>C15</td><td>CapacityRule</td>
 * <td>mixed-dynamic-children-capacity-types</td><td>ERROR</td></tr>
 * <tr><td>C16 to C22</td><td>not evaluated: they need effective resources
 * or live entitlements</td><td></td><td></td></tr>
 * <tr><td>L01, L02</td><td>NodeLabelRule</td><td>child-labels-not-subset</td>
 * <td>ERROR</td></tr>
 * <tr><td>L03</td><td>NodeLabelRule</td>
 * <td>invalid-default-label-expression</td><td>ERROR</td></tr>
 * <tr><td>L04</td><td>NodeLabelRule</td>
 * <td>root-accessible-labels-ignored</td><td>WARNING</td></tr>
 * <tr><td>L05</td><td>NodeLabelRule</td><td>invalid-template-label</td>
 * <td>ERROR, WARNING when the parent accesses any label</td></tr>
 * <tr><td>labeled key without a property</td><td>NodeLabelRule</td>
 * <td>invalid-node-label-key</td><td>ERROR</td></tr>
 * <tr><td>S01</td><td>StateRule</td><td>invalid-state</td><td>ERROR</td></tr>
 * <tr><td>S02</td><td>StateRule</td>
 * <td>running-queue-under-stopped-parent</td><td>ERROR</td></tr>
 * <tr><td>S03</td><td>StateRule</td><td>parent-queue-not-running</td>
 * <td>ERROR</td></tr>
 * <tr><td>S04</td><td>HierarchyRule</td><td>queue-removal-not-stopped</td>
 * <td>ERROR</td></tr>
 * <tr><td>S05</td><td>HierarchyRule</td><td>leaf-to-parent-conversion</td>
 * <td>ERROR</td></tr>
 * <tr><td>S06, S07</td><td>HierarchyRule</td>
 * <td>managed-parent-conversion</td><td>ERROR</td></tr>
 * <tr><td>S08, S09</td><td>not evaluated: unreachable, or only for queue
 * classes a refresh cannot swap</td><td></td><td></td></tr>
 * <tr><td>S10, S11</td><td>not evaluated: application recovery</td>
 * <td></td><td></td></tr>
 * <tr><td>H01</td><td>StructureRule</td><td>missing-child-queues</td>
 * <td>ERROR</td></tr>
 * <tr><td>H02</td><td>StructureRule</td><td>reservable-parent-queue</td>
 * <td>ERROR</td></tr>
 * <tr><td>H03</td><td>none, unreachable</td><td></td><td></td></tr>
 * <tr><td>H04</td><td>StructureRule</td><td>managed-parent-template-type</td>
 * <td>ERROR</td></tr>
 * <tr><td>H05 to H08</td><td>the validate endpoint</td>
 * <td>invalid-mutation</td><td>ERROR</td></tr>
 * <tr><td>H09</td><td>not evaluated: mutation ACLs of the caller</td>
 * <td></td><td></td></tr>
 * <tr><td>A01</td><td>AllocationRule</td><td>invalid-memory-allocation</td>
 * <td>ERROR</td></tr>
 * <tr><td>A02</td><td>AllocationRule</td><td>invalid-vcores-allocation</td>
 * <td>ERROR</td></tr>
 * <tr><td>A03, A04</td><td>AllocationRule</td>
 * <td>maximum-allocation-exceeds-cluster</td><td>ERROR</td></tr>
 * <tr><td>A05</td><td>AllocationRule</td><td>invalid-maximum-allocation</td>
 * <td>ERROR</td></tr>
 * <tr><td>A06</td><td>AllocationRule</td><td>maximum-allocation-decreased</td>
 * <td>ERROR</td></tr>
 * <tr><td>A07</td><td>not evaluated: startup only</td><td></td><td></td></tr>
 * <tr><td>U01</td><td>LimitRule</td><td>user-weight-out-of-range</td>
 * <td>ERROR</td></tr>
 * <tr><td>U02</td><td>LimitRule</td><td>invalid-user-weight</td>
 * <td>ERROR</td></tr>
 * <tr><td>U03</td><td>LimitRule</td><td>default-lifetime-exceeds-maximum</td>
 * <td>ERROR</td></tr>
 * <tr><td>U04</td><td>ValueRule</td><td>invalid-priority-acl</td>
 * <td>ERROR</td></tr>
 * <tr><td>U05</td><td>not evaluated: the parser caps the value and warns
 * </td><td></td><td></td></tr>
 * <tr><td>U06</td><td>ValueRule</td><td>invalid-ordering-policy</td>
 * <td>ERROR</td></tr>
 * <tr><td>U07</td><td>ValueRule</td><td>invalid-queue-ordering-policy</td>
 * <td>ERROR</td></tr>
 * <tr><td>U08</td><td>ValueRule</td><td>invalid-multi-node-policy</td>
 * <td>ERROR</td></tr>
 * <tr><td>U09</td><td>not evaluated: startup only</td><td></td><td></td></tr>
 * <tr><td>U10</td><td>ValueRule</td><td>offswitch-assignments-below-one</td>
 * <td>WARNING</td></tr>
 * <tr><td>U11</td><td>ValueRule</td><td>invalid-queue-management-policy</td>
 * <td>ERROR</td></tr>
 * <tr><td>U12</td><td>ValueRule</td><td>invalid-value</td><td>ERROR, WARNING
 * for values read only at dynamic queue creation or by the reservation
 * system</td></tr>
 * <tr><td>P01</td><td>PlacementRulesRule</td>
 * <td>duplicate-placement-rules</td><td>ERROR</td></tr>
 * <tr><td>P02, P03</td><td>PlacementRulesRule</td>
 * <td>invalid-placement-rule</td><td>ERROR</td></tr>
 * <tr><td>P04 to P06</td><td>PlacementRulesRule</td>
 * <td>invalid-mapping-rules</td><td>ERROR</td></tr>
 * <tr><td>P07</td><td>PlacementRulesRule</td>
 * <td>mapping-rule-json-missing</td><td>WARNING</td></tr>
 * <tr><td>P08 to P13</td><td>PlacementRulesRule</td>
 * <td>invalid-mapping-rule-target</td><td>ERROR</td></tr>
 * <tr><td>P14</td><td>PlacementRulesRule</td>
 * <td>invalid-workflow-priority-mapping</td><td>ERROR</td></tr>
 * <tr><td>D01 to D17</td><td>not evaluated: dynamic queue operations</td>
 * <td></td><td></td></tr>
 * <tr><td>G01</td><td>not evaluated: startup only</td><td></td><td></td></tr>
 * </table>
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
