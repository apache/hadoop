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
package org.apache.hadoop.mapreduce.v2.app.security.authorize;

import org.apache.hadoop.security.AccessControlException;

/**
 * Exception thrown when a MapReduce job violates the Task-Level Security policy.
 */
public class TaskLevelSecurityException extends AccessControlException {

  /**
   * Constructs a new TaskLevelSecurityException describing the specific policy violation.
   *
   * @param user the submitting user
   * @param property the MapReduce configuration key that was checked
   * @param propertyValue the value provided for that configuration property
   * @param deniedTask the blacklist entry that the value matched
   */
  public TaskLevelSecurityException(
      String user, String property, String propertyValue, String deniedTask
  ) {
    super(String.format(
        "The %s is not allowed to use %s = %s config, cause it match with %s denied task",
        user, property, propertyValue, deniedTask
    ));
  }
}
