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

/**
 * Class captures information of a storage in Datanode.
 */
public class DatanodeStorage {
  /** The state of the storage. */
  public enum State {
    NORMAL,
    READ_ONLY
  }
  
  private final String storageID;
  private final State state;

  /**
   * Create a storage with {@link State#NORMAL}.
   * @param storageID
   */
  public DatanodeStorage(String storageID) {
    this(storageID, State.NORMAL);
  }

  public DatanodeStorage(String sid, State s) {
    storageID = sid;
    state = s;
  }

  public String getStorageID() {
    return storageID;
  }

  public State getState() {
    return state;
  }
}
