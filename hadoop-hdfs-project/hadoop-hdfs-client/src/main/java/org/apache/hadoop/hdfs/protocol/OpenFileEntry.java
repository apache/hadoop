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

package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An open file entry for use by DFSAdmin commands.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OpenFileEntry {
  private final long id;
  private final String filePath;
  private final String clientName;
  private final String clientMachine;

  public OpenFileEntry(long id, String filePath,
      String clientName, String clientMachine) {
    this.id = id;
    this.filePath = filePath;
    this.clientName = clientName;
    this.clientMachine = clientMachine;
  }

  public long getId() {
    return id;
  }

  public String getFilePath() {
    return filePath;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  public String getClientName() {
    return clientName;
  }
}
