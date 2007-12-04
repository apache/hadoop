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
package org.apache.hadoop.security;

import org.apache.hadoop.io.Writable;

/** A {@link Writable} interface for storing user and groups information.
 */
public interface UserGroupInformation extends Writable {
  /** Get username
   * 
   * @return the user's name
   */
  public String getUserName();
  
  /** Get the name of the groups that the user belong to
   * 
   * @return an array of group names
   */
  public String[] getGroupNames();
  
  /** Get the default group name.
   * 
   * @return the default the group name
   */
  public String getDefaultGroupName();
}
