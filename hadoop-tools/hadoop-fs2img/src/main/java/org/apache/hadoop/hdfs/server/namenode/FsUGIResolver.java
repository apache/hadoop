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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.HashSet;
import java.util.Set;

/**
 * Dynamically assign ids to users/groups as they appear in the external
 * filesystem.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FsUGIResolver extends UGIResolver {

  private int id;
  private final Set<String> usernames;
  private final Set<String> groupnames;

  FsUGIResolver() {
    super();
    id = 0;
    usernames = new HashSet<String>();
    groupnames = new HashSet<String>();
  }

  @Override
  public synchronized void addUser(String name) {
    if (!usernames.contains(name)) {
      addUser(name, id);
      id++;
      usernames.add(name);
    }
  }

  @Override
  public synchronized void addGroup(String name) {
    if (!groupnames.contains(name)) {
      addGroup(name, id);
      id++;
      groupnames.add(name);
    }
  }

}
