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

package org.apache.hadoop.fs.gs;

import org.apache.hadoop.fs.statistics.StoreStatisticNames;

import static org.apache.hadoop.fs.gs.StatisticTypeEnum.TYPE_DURATION;

enum GcsStatistics {
  INVOCATION_GET_FILE_STATUS(
      StoreStatisticNames.OP_GET_FILE_STATUS,
      "Calls of getFileStatus()",
      TYPE_DURATION),
  INVOCATION_CREATE(
      StoreStatisticNames.OP_CREATE,
      "Calls of create()",
      TYPE_DURATION),
  INVOCATION_DELETE(
      StoreStatisticNames.OP_DELETE,
      "Calls of delete()",
      TYPE_DURATION),
  INVOCATION_RENAME(
      StoreStatisticNames.OP_RENAME,
      "Calls of rename()",
      TYPE_DURATION),
  INVOCATION_OPEN(
      StoreStatisticNames.OP_OPEN,
      "Calls of open()",
      TYPE_DURATION),
  INVOCATION_MKDIRS(
      StoreStatisticNames.OP_MKDIRS,
      "Calls of mkdirs()",
      TYPE_DURATION),
  INVOCATION_LIST_STATUS(
      StoreStatisticNames.OP_LIST_STATUS,
      "Calls of listStatus()",
      TYPE_DURATION);

  private final String description;
  private final StatisticTypeEnum type;
  private final String symbol;

  StatisticTypeEnum getType() {
    return this.type;
  }

  String getSymbol() {
    return this.symbol;
  }

  GcsStatistics(String symbol, String description, StatisticTypeEnum type) {
    this.symbol = symbol;
    this.description = description;
    this.type = type;
  }
}
