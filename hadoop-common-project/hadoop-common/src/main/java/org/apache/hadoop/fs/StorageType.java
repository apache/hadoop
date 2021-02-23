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

package org.apache.hadoop.fs;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Defines the types of supported storage media. The default storage
 * medium is assumed to be DISK.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum StorageType {
  RAM_DISK(true, true),
  SSD(false, false),
  DISK(false, false),
  ARCHIVE(false, false),
  PROVIDED(false, false),
  NVDIMM(false, true);

  private final boolean isTransient;
  private final boolean isRAM;

  public static final StorageType DEFAULT = DISK;

  public static final StorageType[] EMPTY_ARRAY = {};

  private static final StorageType[] VALUES = values();

  StorageType(boolean isTransient, boolean isRAM) {
    this.isTransient = isTransient;
    this.isRAM = isRAM;
  }

  public boolean isTransient() {
    return isTransient;
  }

  public boolean isRAM() {
    return isRAM;
  }

  public boolean supportTypeQuota() {
    return !isTransient;
  }

  public boolean isMovable() {
    return !isTransient;
  }

  public static List<StorageType> asList() {
    return Arrays.asList(VALUES);
  }

  public static List<StorageType> getMovableTypes() {
    return getNonTransientTypes();
  }

  public static List<StorageType> getTypesSupportingQuota() {
    return getNonTransientTypes();
  }

  public static StorageType parseStorageType(int i) {
    return VALUES[i];
  }

  public static StorageType parseStorageType(String s) {
    return StorageType.valueOf(StringUtils.toUpperCase(s));
  }

  public static boolean allowSameDiskTiering(StorageType storageType) {
    return storageType == StorageType.DISK
        || storageType == StorageType.ARCHIVE;
  }

  private static List<StorageType> getNonTransientTypes() {
    List<StorageType> nonTransientTypes = new ArrayList<>();
    for (StorageType t : VALUES) {
      if ( t.isTransient == false ) {
        nonTransientTypes.add(t);
      }
    }
    return nonTransientTypes;
  }

  // The configuration header for different StorageType.
  public static final String CONF_KEY_HEADER =
      "dfs.datanode.storagetype.";

  /**
   * Get the configured values for different StorageType.
   * @param conf - absolute or fully qualified path
   * @param t - the StorageType
   * @param name - the sub-name of key
   * @return the file system of the path
   */
  public static String getConf(Configuration conf,
                               StorageType t, String name) {
    return conf.get(CONF_KEY_HEADER + t.toString() + "." + name);
  }
}
