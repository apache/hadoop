/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

/**
 * A class used to encapsulate a single OM DB update event.
 * Currently only PUT and DELETE are supported.
 * @param <KEY> Type of Key.
 * @param <VALUE> Type of Value.
 */
public final class OMDBUpdateEvent<KEY, VALUE> {

  private final OMDBUpdateAction action;
  private final String table;
  private final KEY updatedKey;
  private final VALUE updatedValue;
  private final EventInfo eventInfo;

  private OMDBUpdateEvent(OMDBUpdateAction action,
                          String table,
                          KEY updatedKey,
                          VALUE updatedValue,
                          EventInfo eventInfo) {
    this.action = action;
    this.table = table;
    this.updatedKey = updatedKey;
    this.updatedValue = updatedValue;
    this.eventInfo = eventInfo;
  }

  public OMDBUpdateAction getAction() {
    return action;
  }

  public String getTable() {
    return table;
  }

  public KEY getKey() {
    return updatedKey;
  }

  public VALUE getValue() {
    return updatedValue;
  }

  public EventInfo getEventInfo() {
    return eventInfo;
  }

  /**
   * Builder used to construct an OM DB Update event.
   * @param <KEY> Key type.
   * @param <VALUE> Value type.
   */
  public static class OMUpdateEventBuilder<KEY, VALUE> {

    private OMDBUpdateAction action;
    private String table;
    private KEY updatedKey;
    private VALUE updatedValue;
    private EventInfo eventInfo;

    OMUpdateEventBuilder setAction(OMDBUpdateAction omdbUpdateAction) {
      this.action = omdbUpdateAction;
      return this;
    }

    OMUpdateEventBuilder setTable(String tableName) {
      this.table = tableName;
      return this;
    }

    OMUpdateEventBuilder setKey(KEY key) {
      this.updatedKey = key;
      return this;
    }

    OMUpdateEventBuilder setValue(VALUE value) {
      this.updatedValue = value;
      return this;
    }

    OMUpdateEventBuilder setEventInfo(long sequenceNumber,
                                      long eventTimestampMillis) {
      this.eventInfo = new EventInfo(sequenceNumber,
          eventTimestampMillis);
      return this;
    }

    /**
     * Build an OM update event.
     * @return OMDBUpdateEvent
     */
    public OMDBUpdateEvent build() {
      return new OMDBUpdateEvent<KEY, VALUE>(
          action,
          table,
          updatedKey,
          updatedValue,
          eventInfo);
    }
  }

  /**
   * Class used to hold timing information for an event. (Seq number and
   * timestamp)
   */
  public static class EventInfo {
    private long sequenceNumber;
    private long eventTimestampMillis;

    public EventInfo(long sequenceNumber,
                     long eventTimestampMillis) {
      this.sequenceNumber = sequenceNumber;
      this.eventTimestampMillis = eventTimestampMillis;
    }

    public long getSequenceNumber() {
      return sequenceNumber;
    }

    public long getEventTimestampMillis() {
      return eventTimestampMillis;
    }
  }

  /**
   * Supported Actions - PUT, DELETE.
   */
  public enum OMDBUpdateAction {
    PUT, DELETE
  }
}
