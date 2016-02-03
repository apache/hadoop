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

import org.apache.hadoop.fs.StorageType;

import java.util.UUID;

/**
 * Class captures information of a storage in Datanode.
 */
public class DatanodeStorage {
  /** The state of the storage. */
  public enum State {
    NORMAL,

    /**
     * A storage that represents a read-only path to replicas stored on a shared
     * storage device. Replicas on {@link #READ_ONLY_SHARED} storage are not
     * counted towards live replicas.
     *
     * <p>
     * In certain implementations, a {@link #READ_ONLY_SHARED} storage may be
     * correlated to its {@link #NORMAL} counterpart using the
     * {@link DatanodeStorage#storageID}.  This property should be used for
     * debugging purposes only.
     * </p>
     */
    READ_ONLY_SHARED,

    FAILED
  }

  private final String storageID;
  private final State state;
  private final StorageType storageType;
  private static final String STORAGE_ID_PREFIX = "DS-";

  /**
   * Create a storage with {@link State#NORMAL} and {@link StorageType#DEFAULT}.
   */
  public DatanodeStorage(String storageID) {
    this(storageID, State.NORMAL, StorageType.DEFAULT);
  }

  public DatanodeStorage(String sid, State s, StorageType sm) {
    this.storageID = sid;
    this.state = s;
    this.storageType = sm;
  }

  public String getStorageID() {
    return storageID;
  }

  public State getState() {
    return state;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Generate new storage ID. The format of this string can be changed
   * in the future without requiring that old storage IDs be updated.
   *
   * @return unique storage ID
   */
  public static String generateUuid() {
    return STORAGE_ID_PREFIX + UUID.randomUUID();
  }

  /**
   * Verify that a given string is a storage ID in the "DS-..uuid.." format.
   */
  public static boolean isValidStorageId(final String storageID) {
    try {
      // Attempt to parse the UUID.
      if (storageID != null && storageID.indexOf(STORAGE_ID_PREFIX) == 0) {
        UUID.fromString(storageID.substring(STORAGE_ID_PREFIX.length()));
        return true;
      }
    } catch (IllegalArgumentException ignored) {
    }

    return false;
  }

  @Override
  public String toString() {
    return "DatanodeStorage["+ storageID + "," + storageType + "," + state +"]";
  }

  @Override
  public boolean equals(Object other){
    if (other == this) {
      return true;
    }

    if ((other == null) ||
        !(other instanceof DatanodeStorage)) {
      return false;
    }
    DatanodeStorage otherStorage = (DatanodeStorage) other;
    return otherStorage.getStorageID().compareTo(getStorageID()) == 0;
  }

  @Override
  public int hashCode() {
    return getStorageID().hashCode();
  }
}
