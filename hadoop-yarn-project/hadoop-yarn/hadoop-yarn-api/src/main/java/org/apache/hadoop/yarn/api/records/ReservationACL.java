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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;

/**
 * {@code ReservationACL} enumerates the various ACLs for reservations.
 * <p>
 * The ACL is one of:
 * <ul>
 *   <li>
 *     {@link #ADMINISTER_RESERVATIONS} - ACL to create, list, update and
 *     delete reservations.
 *   </li>
 *   <li> {@link #LIST_RESERVATIONS} - ACL to list reservations. </li>
 *   <li> {@link #SUBMIT_RESERVATIONS} - ACL to create reservations. </li>
 * </ul>
 * Users can always list, update and delete their own reservations.
 */
@Public
@Stable
public enum ReservationACL {
  /**
   * ACL to create, list, update and delete reservations.
   */
  ADMINISTER_RESERVATIONS,

  /**
   * ACL to list reservations.
   */
  LIST_RESERVATIONS,

  /**
   * ACL to create reservations.
   */
  SUBMIT_RESERVATIONS

}