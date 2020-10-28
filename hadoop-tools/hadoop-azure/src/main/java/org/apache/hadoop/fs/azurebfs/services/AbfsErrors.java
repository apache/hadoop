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
package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * ABFS error constants.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AbfsErrors {
  public static final String ERR_WRITE_WITHOUT_LEASE = "Attempted to write to file without lease";
  public static final String ERR_LEASE_EXPIRED = "A lease ID was specified, but the lease for the"
      + " resource has expired.";
  public static final String ERR_PARALLEL_ACCESS_DETECTED = "Parallel access to the create path "
      + "detected. Failing request to honor single writer semantics";
  public static final String ERR_ACQUIRING_LEASE = "Unable to acquire lease";
  public static final String ERR_LEASE_PRESENT = "There is already a lease present.";

  private AbfsErrors() {}
}
