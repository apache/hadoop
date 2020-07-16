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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames;

/**
 * Statistic which are collected in Abfs.
 * Available as metrics in {@link AbfsCountersImpl}.
 */
public enum AbfsStatistic {

  CALL_CREATE(CommonStatisticNames.OP_CREATE,
      "Calls of create()."),
  CALL_OPEN(CommonStatisticNames.OP_OPEN,
      "Calls of open()."),
  CALL_GET_FILE_STATUS(CommonStatisticNames.OP_GET_FILE_STATUS,
      "Calls of getFileStatus()."),
  CALL_APPEND(CommonStatisticNames.OP_APPEND,
      "Calls of append()."),
  CALL_CREATE_NON_RECURSIVE(CommonStatisticNames.OP_CREATE_NON_RECURSIVE,
      "Calls of createNonRecursive()."),
  CALL_DELETE(CommonStatisticNames.OP_DELETE,
      "Calls of delete()."),
  CALL_EXIST(CommonStatisticNames.OP_EXISTS,
      "Calls of exist()."),
  CALL_GET_DELEGATION_TOKEN(CommonStatisticNames.OP_GET_DELEGATION_TOKEN,
      "Calls of getDelegationToken()."),
  CALL_LIST_STATUS(CommonStatisticNames.OP_LIST_STATUS,
      "Calls of listStatus()."),
  CALL_MKDIRS(CommonStatisticNames.OP_MKDIRS,
      "Calls of mkdirs()."),
  CALL_RENAME(CommonStatisticNames.OP_RENAME,
      "Calls of rename()."),
  DIRECTORIES_CREATED("directories_created",
      "Total number of directories created through the object store."),
  DIRECTORIES_DELETED("directories_deleted",
      "Total number of directories deleted through the object store."),
  FILES_CREATED("files_created",
      "Total number of files created through the object store."),
  FILES_DELETED("files_deleted",
      "Total number of files deleted from the object store."),
  ERROR_IGNORED("error_ignored",
      "Errors caught and ignored."),

  //Network statistics.
  CONNECTIONS_MADE("connections_made",
      "Total number of times a connection was made with the data store."),
  SEND_REQUESTS("send_requests",
      "Total number of times http requests were sent to the data store."),
  GET_RESPONSES("get_responses",
      "Total number of times a response was received."),
  BYTES_SENT("bytes_sent",
      "Total bytes uploaded."),
  BYTES_RECEIVED("bytes_received",
      "Total bytes received."),
  READ_THROTTLES("read_throttles",
      "Total number of times a read operation is throttled."),
  WRITE_THROTTLES("write_throttles",
      "Total number of times a write operation is throttled.");

  private String statName;
  private String statDescription;

  /**
   * Constructor of AbfsStatistic to set statistic name and description.
   *
   * @param statName        Name of the statistic.
   * @param statDescription Description of the statistic.
   */
  AbfsStatistic(String statName, String statDescription) {
    this.statName = statName;
    this.statDescription = statDescription;
  }

  /**
   * Getter for statistic name.
   *
   * @return Name of statistic.
   */
  public String getStatName() {
    return statName;
  }

  /**
   * Getter for statistic description.
   *
   * @return Description of statistic.
   */
  public String getStatDescription() {
    return statDescription;
  }
}
