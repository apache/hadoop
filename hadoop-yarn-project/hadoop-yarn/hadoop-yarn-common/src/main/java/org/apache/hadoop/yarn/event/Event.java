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

package org.apache.hadoop.yarn.event;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Interface defining events api.
 *
 */
@Public
@Evolving
public interface Event<TYPE extends Enum<TYPE>> {

  TYPE getType();
  long getTimestamp();
  String toString();

  /**
   * In case of parallel execution of events in the same dispatcher,
   * the result of this method will be used as semaphore.
   * If method returns null, then a default semaphore will be used.
   * @return the semaphore
   */
  default String getLockKey() {
    return null;
  };
}
