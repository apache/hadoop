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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Namenode standby state. In this state the namenode acts as warm standby and
 * keeps the following updated:
 * <ul>
 * <li>Namespace by getting the edits.</li>
 * <li>Block location information by receiving block reports and blocks
 * received from the datanodes.</li>
 * </ul>
 * 
 * It does not handle read/write/checkpoint operations.
 */
public class StandbyState extends HAState {
  public StandbyState() {
    super("standby");
  }

  @Override
  public void setState(NameNode nn, HAState s) throws ServiceFailedException {
    if (s == NameNode.ACTIVE_STATE) {
      setStateInternal(nn, s);
      return;
    }
    super.setState(nn, s);
  }

  @Override
  protected void enterState(NameNode nn) throws ServiceFailedException {
    // TODO:HA
  }

  @Override
  protected void exitState(NameNode nn) throws ServiceFailedException {
    // TODO:HA
  }
}

