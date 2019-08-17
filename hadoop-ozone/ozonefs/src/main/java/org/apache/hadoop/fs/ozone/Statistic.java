/*
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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistic which are collected in OzoneFileSystem.
 * These statistics are available at a low level in
 * {@link OzoneFSStorageStatistics}
 */
public enum Statistic {
  OBJECTS_RENAMED("objects_renamed",
      "Total number of objects renamed within the object store."),
  OBJECTS_CREATED("objects_created",
      "Total number of objects created through the object store."),
  OBJECTS_DELETED("objects_deleted",
      "Total number of objects deleted from the object store."),
  OBJECTS_READ("objects_read",
      "Total number of objects read from the object store."),
  OBJECTS_QUERY("objects_query",
      "Total number of objects queried from the object store."),
  OBJECTS_LIST("objects_list",
      "Total number of object list query from the object store."),
  INVOCATION_COPY_FROM_LOCAL_FILE(CommonStatisticNames.OP_COPY_FROM_LOCAL_FILE,
      "Calls of copyFromLocalFile()"),
  INVOCATION_CREATE(CommonStatisticNames.OP_CREATE,
      "Calls of create()"),
  INVOCATION_CREATE_NON_RECURSIVE(CommonStatisticNames.OP_CREATE_NON_RECURSIVE,
      "Calls of createNonRecursive()"),
  INVOCATION_DELETE(CommonStatisticNames.OP_DELETE,
      "Calls of delete()"),
  INVOCATION_EXISTS(CommonStatisticNames.OP_EXISTS,
      "Calls of exists()"),
  INVOCATION_GET_FILE_CHECKSUM(CommonStatisticNames.OP_GET_FILE_CHECKSUM,
      "Calls of getFileChecksum()"),
  INVOCATION_GET_FILE_STATUS(CommonStatisticNames.OP_GET_FILE_STATUS,
      "Calls of getFileStatus()"),
  INVOCATION_GLOB_STATUS(CommonStatisticNames.OP_GLOB_STATUS,
      "Calls of globStatus()"),
  INVOCATION_IS_DIRECTORY(CommonStatisticNames.OP_IS_DIRECTORY,
      "Calls of isDirectory()"),
  INVOCATION_IS_FILE(CommonStatisticNames.OP_IS_FILE,
      "Calls of isFile()"),
  INVOCATION_LIST_FILES(CommonStatisticNames.OP_LIST_FILES,
      "Calls of listFiles()"),
  INVOCATION_LIST_LOCATED_STATUS(CommonStatisticNames.OP_LIST_LOCATED_STATUS,
      "Calls of listLocatedStatus()"),
  INVOCATION_LIST_STATUS(CommonStatisticNames.OP_LIST_STATUS,
      "Calls of listStatus()"),
  INVOCATION_MKDIRS(CommonStatisticNames.OP_MKDIRS,
      "Calls of mkdirs()"),
  INVOCATION_OPEN(CommonStatisticNames.OP_OPEN,
      "Calls of open()"),
  INVOCATION_RENAME(CommonStatisticNames.OP_RENAME,
      "Calls of rename()");

  private static final Map<String, Statistic> SYMBOL_MAP =
      new HashMap<>(Statistic.values().length);
  static {
    for (Statistic stat : values()) {
      SYMBOL_MAP.put(stat.getSymbol(), stat);
    }
  }

  Statistic(String symbol, String description) {
    this.symbol = symbol;
    this.description = description;
  }

  private final String symbol;
  private final String description;

  public String getSymbol() {
    return symbol;
  }

  /**
   * Get a statistic from a symbol.
   * @param symbol statistic to look up
   * @return the value or null.
   */
  public static Statistic fromSymbol(String symbol) {
    return SYMBOL_MAP.get(symbol);
  }

  public String getDescription() {
    return description;
  }

  /**
   * The string value is simply the symbol.
   * This makes this operation very low cost.
   * @return the symbol of this statistic.
   */
  @Override
  public String toString() {
    return symbol;
  }
}
