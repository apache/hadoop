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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;

/**
 * Base class for data-node command.
 * Issued by the name-node to notify data-nodes what should be done.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class DatanodeCommandWritable extends ServerCommandWritable {
  public DatanodeCommandWritable() {
    super();
  }
  
  DatanodeCommandWritable(int action) {
    super(action);
  }

  /** Method to convert from writable type to internal type */
  public abstract DatanodeCommand convert();

  public static DatanodeCommandWritable[] convert(DatanodeCommand[] cmds) {
    DatanodeCommandWritable[] ret = new DatanodeCommandWritable[cmds.length];
    for (int i = 0; i < cmds.length; i++) {
      ret[i] = DatanodeCommandHelper.convert(cmds[i]);
    }
    return ret;
  }

  public static DatanodeCommand[] convert(DatanodeCommandWritable[] cmds) {
    if (cmds == null) return null;
    DatanodeCommand[] ret = new DatanodeCommand[cmds.length];
    for (int i = 0; i < cmds.length; i++) {
      ret[i] = cmds[i].convert();
    }
    return ret;
  }
}
