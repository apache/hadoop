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

package org.apache.hadoop.registry.client.binding;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.util.JsonSerialization;

import java.io.EOFException;
import java.io.IOException;

/**
 * Support for marshalling objects to and from JSON.
 *  <p>
 * This extends {@link JsonSerialization} with the notion
 * of a marker field in the JSON file, with
 * <ol>
 *   <li>a fail-fast check for it before even trying to parse.</li>
 *   <li>Specific IOException subclasses for a failure.</li>
 * </ol>
 * The rationale for this is not only to support different things in the,
 * registry, but the fact that all ZK nodes have a size &gt; 0 when examined.
 *
 * @param <T> Type to marshal.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JsonSerDeser<T> extends JsonSerialization<T> {

  private static final String UTF_8 = "UTF-8";
  public static final String E_NO_DATA = "No data at path";
  public static final String E_DATA_TOO_SHORT = "Data at path too short";
  public static final String E_MISSING_MARKER_STRING =
      "Missing marker string: ";

  /**
   * Create an instance bound to a specific type
   * @param classType class to marshall
   */
  public JsonSerDeser(Class<T> classType) {
    super(classType, false, false);
  }

  /**
   * Deserialize from a byte array
   * @param path path the data came from
   * @param bytes byte array
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the parsing failed -the record is invalid
   * @throws NoRecordException if the data is not considered a record: either
   * it is too short or it did not contain the marker string.
   */
  public T fromBytes(String path, byte[] bytes) throws IOException {
    return fromBytes(path, bytes, "");
  }

  /**
   * Deserialize from a byte array, optionally checking for a marker string.
   * <p>
   * If the marker parameter is supplied (and not empty), then its presence
   * will be verified before the JSON parsing takes place; it is a fast-fail
   * check. If not found, an {@link InvalidRecordException} exception will be
   * raised
   * @param path path the data came from
   * @param bytes byte array
   * @param marker an optional string which, if set, MUST be present in the
   * UTF-8 parsed payload.
   * @return The parsed record
   * @throws IOException all problems
   * @throws EOFException not enough data
   * @throws InvalidRecordException if the JSON parsing failed.
   * @throws NoRecordException if the data is not considered a record: either
   * it is too short or it did not contain the marker string.
   */
  public T fromBytes(String path, byte[] bytes, String marker)
      throws IOException {
    int len = bytes.length;
    if (len == 0 ) {
      throw new NoRecordException(path, E_NO_DATA);
    }
    if (StringUtils.isNotEmpty(marker) && len < marker.length()) {
      throw new NoRecordException(path, E_DATA_TOO_SHORT);
    }
    String json = new String(bytes, 0, len, UTF_8);
    if (StringUtils.isNotEmpty(marker)
        && !json.contains(marker)) {
      throw new NoRecordException(path, E_MISSING_MARKER_STRING + marker);
    }
    try {
      return fromJson(json);
    } catch (JsonProcessingException e) {
      throw new InvalidRecordException(path, e.toString(), e);
    }
  }

}
