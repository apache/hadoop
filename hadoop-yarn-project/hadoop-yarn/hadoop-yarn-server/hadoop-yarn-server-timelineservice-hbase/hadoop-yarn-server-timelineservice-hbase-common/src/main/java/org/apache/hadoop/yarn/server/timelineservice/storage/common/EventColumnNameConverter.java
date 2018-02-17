/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Encodes and decodes event column names for application and entity tables.
 * The event column name is of the form : eventId=timestamp=infokey.
 * If info is not associated with the event, event column name is of the form :
 * eventId=timestamp=
 * Event timestamp is long and rest are strings.
 * Column prefixes are not part of the eventcolumn name passed for encoding. It
 * is added later, if required in the associated ColumnPrefix implementations.
 */
public final class EventColumnNameConverter
    implements KeyConverter<EventColumnName> {

  public EventColumnNameConverter() {
  }

  // eventId=timestamp=infokey are of types String, Long String
  // Strings are variable in size (i.e. end whenever separator is encountered).
  // This is used while decoding and helps in determining where to split.
  private static final int[] SEGMENT_SIZES = {
      Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG, Separator.VARIABLE_SIZE };

  /*
   * (non-Javadoc)
   *
   * Encodes EventColumnName into a byte array with each component/field in
   * EventColumnName separated by Separator#VALUES. This leads to an event
   * column name of the form eventId=timestamp=infokey.
   * If timestamp in passed EventColumnName object is null (eventId is not null)
   * this returns a column prefix of the form eventId= and if infokey in
   * EventColumnName is null (other 2 components are not null), this returns a
   * column name of the form eventId=timestamp=
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(EventColumnName key) {
    byte[] first = Separator.encode(key.getId(), Separator.SPACE, Separator.TAB,
        Separator.VALUES);
    if (key.getTimestamp() == null) {
      return Separator.VALUES.join(first, Separator.EMPTY_BYTES);
    }
    byte[] second = Bytes.toBytes(
        LongConverter.invertLong(key.getTimestamp()));
    if (key.getInfoKey() == null) {
      return Separator.VALUES.join(first, second, Separator.EMPTY_BYTES);
    }
    return Separator.VALUES.join(first, second, Separator.encode(
        key.getInfoKey(), Separator.SPACE, Separator.TAB, Separator.VALUES));
  }

  /*
   * (non-Javadoc)
   *
   * Decodes an event column name of the form eventId=timestamp= or
   * eventId=timestamp=infoKey represented in byte format and converts it into
   * an EventColumnName object.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public EventColumnName decode(byte[] bytes) {
    byte[][] components = Separator.VALUES.split(bytes, SEGMENT_SIZES);
    if (components.length != 3) {
      throw new IllegalArgumentException("the column name is not valid");
    }
    String id = Separator.decode(Bytes.toString(components[0]),
        Separator.VALUES, Separator.TAB, Separator.SPACE);
    Long ts = LongConverter.invertLong(Bytes.toLong(components[1]));
    String infoKey = components[2].length == 0 ? null :
        Separator.decode(Bytes.toString(components[2]),
            Separator.VALUES, Separator.TAB, Separator.SPACE);
    return new EventColumnName(id, ts, infoKey);
  }
}
