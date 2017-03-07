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
package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.commands.NullCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Command Queue is queue of commands for the datanode.
 * <p>
 * Node manager, container Manager and key space managers can queue commands for
 * datanodes into this queue. These commands will be send in the order in which
 * there where queued.
 */
public class CommandQueue {

  private final Map<DatanodeID, List<SCMCommand>> commandMap;
  private final Lock lock;
  // This map is used as default return value containing one null command.
  private static final List<SCMCommand> DEFAULT_LIST = new LinkedList<>();

  /**
   * Constructs a Command Queue.
   */
  public CommandQueue() {
    commandMap = new HashMap<>();
    lock = new ReentrantLock();
    DEFAULT_LIST.add(NullCommand.newBuilder().build());
  }

  /**
   * Returns  a list of Commands for the datanode to execute, if we have no
   * commands returns a list with Null Command otherwise the current set of
   * commands are returned and command map set to empty list again.
   *
   * @param datanodeID DatanodeID
   * @return List of SCM Commands.
   */
  @SuppressWarnings("unchecked")
  List<SCMCommand> getCommand(final DatanodeID datanodeID) {
    lock.lock();

    try {
      if (commandMap.containsKey(datanodeID)) {
        List temp = commandMap.get(datanodeID);
        if (temp.size() > 0) {
          LinkedList<SCMCommand> emptyList = new LinkedList<>();
          commandMap.put(datanodeID, emptyList);
          return temp;
        }
      }
    } finally {
      lock.unlock();
    }
    return DEFAULT_LIST;
  }

  /**
   * Adds a Command to the SCM Queue to send the command to container.
   *
   * @param datanodeID DatanodeID
   * @param command    - Command
   */
  void addCommand(final DatanodeID datanodeID, final SCMCommand command) {
    lock.lock();
    try {
      if (commandMap.containsKey(datanodeID)) {
        commandMap.get(datanodeID).add(command);
      } else {
        LinkedList<SCMCommand> newList = new LinkedList<>();
        newList.add(command);
        commandMap.put(datanodeID, newList);
      }
    } finally {
      lock.unlock();
    }
  }

}
