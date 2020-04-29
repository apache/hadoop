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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A DropSPSWorkCommand is an instruction to a datanode to drop the SPSWorker's
 * pending block storage movement queues.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DropSPSWorkCommand extends DatanodeCommand {
  public static final DropSPSWorkCommand DNA_DROP_SPS_WORK_COMMAND =
      new DropSPSWorkCommand();

  public DropSPSWorkCommand() {
    super(DatanodeProtocol.DNA_DROP_SPS_WORK_COMMAND);
  }
}
