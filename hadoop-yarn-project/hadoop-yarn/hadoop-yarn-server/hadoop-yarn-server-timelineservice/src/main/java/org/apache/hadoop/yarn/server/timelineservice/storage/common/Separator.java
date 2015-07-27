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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Used to separate row qualifiers, column qualifiers and compount fields.
 */
public enum Separator {

  /**
   * separator in key or column qualifier fields
   */
  QUALIFIERS("!", "%0$"),

  /**
   * separator in values, and/or compound key/column qualifier fields.
   */
  VALUES("?", "%1$"),

  /**
   * separator in values, often used to avoid having these in qualifiers and
   * names. Note that if we use HTML form encoding through URLEncoder, we end up
   * getting a + for a space, which may already occur in strings, so we don't
   * want that.
   */
  SPACE(" ", "%2$");

  /**
   * The string value of this separator.
   */
  private final String value;

  /**
   * The URLEncoded version of this separator
   */
  private final String encodedValue;

  /**
   * The bye representation of value.
   */
  private final byte[] bytes;

  /**
   * The value quoted so that it can be used as a safe regex
   */
  private final String quotedValue;

  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * @param value of the separator to use. Cannot be null or empty string.
   * @param encodedValue choose something that isn't likely to occur in the data
   *          itself. Cannot be null or empty string.
   */
  private Separator(String value, String encodedValue) {
    this.value = value;
    this.encodedValue = encodedValue;

    // validation
    if (value == null || value.length() == 0 || encodedValue == null
        || encodedValue.length() == 0) {
      throw new IllegalArgumentException(
          "Cannot create separator from null or empty string.");
    }

    this.bytes = Bytes.toBytes(value);
    this.quotedValue = Pattern.quote(value);
  }

  /**
   * @return the original value of the separator
   */
  public String getValue() {
    return value;
  }

  /**
   * Used to make token safe to be used with this separator without collisions.
   *
   * @param token
   * @return the token with any occurrences of this separator URLEncoded.
   */
  public String encode(String token) {
    if (token == null || token.length() == 0) {
      // Nothing to replace
      return token;
    }
    return token.replace(value, encodedValue);
  }

  /**
   * @param token
   * @return the token with any occurrences of the encoded separator replaced by
   *         the separator itself.
   */
  public String decode(String token) {
    if (token == null || token.length() == 0) {
      // Nothing to replace
      return token;
    }
    return token.replace(encodedValue, value);
  }

  /**
   * Encode the given separators in the token with their encoding equivalent.
   * This means that when encoding is already present in the token itself, this
   * is not a reversible process. See also {@link #decode(String, Separator...)}
   *
   * @param token containing possible separators that need to be encoded.
   * @param separators to be encoded in the token with their URLEncoding
   *          equivalent.
   * @return non-null byte representation of the token with occurrences of the
   *         separators encoded.
   */
  public static byte[] encode(String token, Separator... separators) {
    if (token == null) {
      return EMPTY_BYTES;
    }
    String result = token;
    for (Separator separator : separators) {
      if (separator != null) {
        result = separator.encode(result);
      }
    }
    return Bytes.toBytes(result);
  }

  /**
   * Decode the given separators in the token with their decoding equivalent.
   * This means that when encoding is already present in the token itself, this
   * is not a reversible process.
   *
   * @param token containing possible separators that need to be encoded.
   * @param separators to be encoded in the token with their URLEncoding
   *          equivalent.
   * @return String representation of the token with occurrences of the URL
   *         encoded separators decoded.
   */
  public static String decode(byte[] token, Separator... separators) {
    if (token == null) {
      return null;
    }
    return decode(Bytes.toString(token), separators);
  }

  /**
   * Decode the given separators in the token with their decoding equivalent.
   * This means that when encoding is already present in the token itself, this
   * is not a reversible process.
   *
   * @param token containing possible separators that need to be encoded.
   * @param separators to be encoded in the token with their URLEncoding
   *          equivalent.
   * @return String representation of the token with occurrences of the URL
   *         encoded separators decoded.
   */
  public static String decode(String token, Separator... separators) {
    if (token == null) {
      return null;
    }
    String result = token;
    for (Separator separator : separators) {
      if (separator != null) {
        result = separator.decode(result);
      }
    }
    return result;
  }

  /**
   * Returns a single byte array containing all of the individual arrays
   * components separated by this separator.
   *
   * @param components
   * @return byte array after joining the components
   */
  public byte[] join(byte[]... components) {
    if (components == null || components.length == 0) {
      return EMPTY_BYTES;
    }

    int finalSize = 0;
    finalSize = this.value.length() * (components.length - 1);
    for (byte[] comp : components) {
      if (comp != null) {
        finalSize += comp.length;
      }
    }

    byte[] buf = new byte[finalSize];
    int offset = 0;
    for (int i = 0; i < components.length; i++) {
      if (components[i] != null) {
        System.arraycopy(components[i], 0, buf, offset, components[i].length);
        offset += components[i].length;
      }
      if (i < (components.length - 1)) {
        System.arraycopy(this.bytes, 0, buf, offset, this.value.length());
        offset += this.value.length();
      }

    }
    return buf;
  }

  /**
   * Concatenates items (as String), using this separator.
   *
   * @param items Items join, {@code toString()} will be called in each item.
   *          Any occurrence of the separator in the individual strings will be
   *          first encoded. Cannot be null.
   * @return non-null joined result. Note that when separator is {@literal null}
   *         the result is simply all items concatenated and the process is not
   *         reversible through {@link #splitEncoded(String)}
   */
  public String joinEncoded(String... items) {
    if (items == null || items.length == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder(encode(items[0].toString()));
    // Start at 1, we've already grabbed the first value at index 0
    for (int i = 1; i < items.length; i++) {
      sb.append(this.value);
      sb.append(encode(items[i].toString()));
    }

    return sb.toString();
  }

  /**
   * Concatenates items (as String), using this separator.
   *
   * @param items Items join, {@code toString()} will be called in each item.
   *          Any occurrence of the separator in the individual strings will be
   *          first encoded. Cannot be null.
   * @return non-null joined result. Note that when separator is {@literal null}
   *         the result is simply all items concatenated and the process is not
   *         reversible through {@link #splitEncoded(String)}
   */
  public String joinEncoded(Iterable<?> items) {
    if (items == null) {
      return "";
    }
    Iterator<?> i = items.iterator();
    if (!i.hasNext()) {
      return "";
    }

    StringBuilder sb = new StringBuilder(encode(i.next().toString()));
    while (i.hasNext()) {
      sb.append(this.value);
      sb.append(encode(i.next().toString()));
    }

    return sb.toString();
  }

  /**
   * @param compoundValue containing individual values separated by this
   *          separator, which have that separator encoded.
   * @return non-null set of values from the compoundValue with the separator
   *         decoded.
   */
  public Collection<String> splitEncoded(String compoundValue) {
    List<String> result = new ArrayList<String>();
    if (compoundValue != null) {
      for (String value : compoundValue.split(quotedValue)) {
        result.add(decode(value));
      }
    }
    return result;
  }

  /**
   * Splits the source array into multiple array segments using this separator,
   * up to a maximum of count items. This will naturally produce copied byte
   * arrays for each of the split segments.
   * @param source to be split
   * @param limit on how many segments are supposed to be returned. Negative
   *          value indicates no limit on number of segments.
   * @return source split by this separator.
   */
  public byte[][] split(byte[] source, int limit) {
    return TimelineWriterUtils.split(source, this.bytes, limit);
  }

}
