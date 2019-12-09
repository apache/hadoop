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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

/**
 * Encapsulates information about Event column names for application and entity
 * tables. Used while encoding/decoding event column names.
 */
public class EventColumnName {

  private final String id;
  private final Long timestamp;
  private final String infoKey;
  private final KeyConverter<EventColumnName> eventColumnNameConverter =
      new EventColumnNameConverter();

  public EventColumnName(String id, Long timestamp, String infoKey) {
    this.id = id;
    this.timestamp = timestamp;
    this.infoKey = infoKey;
  }

  public String getId() {
    return id;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getInfoKey() {
    return infoKey;
  }

  /**
   * @return a byte array with each components/fields separated by
   *         Separator#VALUES. This leads to an event column name of the form
   *         eventId=timestamp=infokey. If both timestamp and infokey are null,
   *         then a qualifier of the form eventId=timestamp= is returned. If
   *         only infokey is null, then a qualifier of the form eventId= is
   *         returned. These prefix forms are useful for queries that intend to
   *         retrieve more than one specific column name.
   */
  public byte[] getColumnQualifier() {
    return eventColumnNameConverter.encode(this);
  }

}
