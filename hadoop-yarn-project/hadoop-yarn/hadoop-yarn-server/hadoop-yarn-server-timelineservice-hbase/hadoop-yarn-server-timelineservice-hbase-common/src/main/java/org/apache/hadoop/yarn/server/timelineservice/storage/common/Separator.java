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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Used to separate row qualifiers, column qualifiers and compound fields.
 */
public enum Separator {

  /**
   * separator in key or column qualifier fields.
   */
  QUALIFIERS("!", "%0$"),

  /**
   * separator in values, and/or compound key/column qualifier fields.
   */
  VALUES("=", "%1$"),

  /**
   * separator in values, often used to avoid having these in qualifiers and
   * names. Note that if we use HTML form encoding through URLEncoder, we end up
   * getting a + for a space, which may already occur in strings, so we don't
   * want that.
   */
  SPACE(" ", "%2$"),

  /**
   * separator in values, often used to avoid having these in qualifiers and
   * names.
   */
  TAB("\t", "%3$");

  // a reserved character that starts each of the encoded values and is encoded
  // first in order to escape naturally occurring instances of encoded values
  // although it can be expressed as an enum instance, we define them as private
  // variables to hide it from callers
  private static final String PERCENT = "%";
  private static final String PERCENT_ENCODED = "%9$";

  private static final Pattern PERCENT_PATTERN =
      Pattern.compile(PERCENT, Pattern.LITERAL);
  private static final String PERCENT_REPLACEMENT =
      Matcher.quoteReplacement(PERCENT);

  private static final Pattern PERCENT_ENCODED_PATTERN =
      Pattern.compile(PERCENT_ENCODED, Pattern.LITERAL);
  private static final String PERCENT_ENCODED_REPLACEMENT =
      Matcher.quoteReplacement(PERCENT_ENCODED);

  /**
   * The string value of this separator.
   */
  private final String value;

  /**
   * The bye representation of value.
   */
  private final byte[] bytes;

  // pre-compiled patterns and quoted replacements for optimization
  private final Pattern valuePattern;
  private final String valueReplacement;

  private final Pattern encodedValuePattern;
  private final String encodedValueReplacement;

  /**
   * Indicator for variable size of an individual segment in a split. The
   * segment ends wherever separator is encountered.
   * Typically used for string.
   * Also used to indicate that there is no fixed number of splits which need to
   * be returned. If split limit is specified as this, all possible splits are
   * returned.
   */
  public static final int VARIABLE_SIZE = 0;


  /** empty string. */
  public static final String EMPTY_STRING = "";

  /** empty bytes. */
  public static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * @param value of the separator to use. Cannot be null or empty string.
   * @param encodedValue choose something that isn't likely to occur in the data
   *          itself. Cannot be null or empty string.
   */
  private Separator(String value, String encodedValue) {
    this.value = value;

    // validation
    if (value == null || value.length() == 0 || encodedValue == null
        || encodedValue.length() == 0) {
      throw new IllegalArgumentException(
          "Cannot create separator from null or empty string.");
    }

    this.bytes = Bytes.toBytes(value);
    this.valuePattern = Pattern.compile(value, Pattern.LITERAL);
    this.valueReplacement = Matcher.quoteReplacement(value);

    this.encodedValuePattern = Pattern.compile(encodedValue, Pattern.LITERAL);
    this.encodedValueReplacement = Matcher.quoteReplacement(encodedValue);
  }

  /**
   * @return the original value of the separator
   */
  public String getValue() {
    return value;
  }

  /**
   * Used to make token safe to be used with this separator without collisions.
   * It <em>must</em> be paired with {@link #decode(String)} for it to be
   * decoded correctly.
   * <p>
   * If you need to encode a given string for multiple separators,
   * {@link #encode(String, Separator...)} should be used over successive
   * invocations of this method. It will result in a more compact version of the
   * encoded value.
   *
   * @param token Token to be encoded.
   * @return the token with any occurrences of this separator URLEncoded.
   */
  public String encode(String token) {
    if (token == null || token.length() == 0) {
      // Nothing to replace
      return token;
    }
    // first encode the percent to escape naturally occurring encoded values
    String escaped = encodePercent(token);
    return encodeSingle(escaped, this);
  }

  private static String replace(String token, Pattern pattern,
      String replacement) {
    return pattern.matcher(token).replaceAll(replacement);
  }

  private static String encodeSingle(String token, Separator separator) {
    return replace(token, separator.valuePattern,
        separator.encodedValueReplacement);
  }

  private static String encodePercent(String token) {
    return replace(token, PERCENT_PATTERN, PERCENT_ENCODED_REPLACEMENT);
  }

  /**
   * Decode the token encoded using {@link #encode(String)}. It <em>must</em> be
   * used for the result encoded with {@link #encode(String)} to be able to
   * recover the original.
   *
   * @param token Token to be decoded.
   * @return the token with any occurrences of the encoded separator replaced by
   *         the separator itself.
   */
  public String decode(String token) {
    if (token == null || token.length() == 0) {
      // Nothing to replace
      return token;
    }
    String escaped = decodeSingle(token, this);
    // decode percent to de-escape
    return decodePercent(escaped);
  }

  private static String decodeSingle(String token, Separator separator) {
    return replace(token, separator.encodedValuePattern,
        separator.valueReplacement);
  }

  private static String decodePercent(String token) {
    return replace(token, PERCENT_ENCODED_PATTERN, PERCENT_REPLACEMENT);
  }

  /**
   * Encode the given separators in the token with their encoding equivalents.
   * It <em>must</em> be paired with {@link #decode(byte[], Separator...)} or
   * {@link #decode(String, Separator...)} with the same separators for it to be
   * decoded correctly.
   * <p>
   * If you need to encode a given string for multiple separators, this form of
   * encoding should be used over successive invocations of
   * {@link #encode(String)}. It will result in a more compact version of the
   * encoded value.
   *
   * @param token containing possible separators that need to be encoded.
   * @param separators to be encoded in the token with their URLEncoding
   *          equivalent.
   * @return non-null byte representation of the token with occurrences of the
   *         separators encoded.
   */
  public static byte[] encode(String token, Separator... separators) {
    if (token == null || token.length() == 0) {
      return EMPTY_BYTES;
    }
    String result = token;
    // first encode the percent to escape naturally occurring encoded values
    result = encodePercent(token);
    for (Separator separator : separators) {
      if (separator != null) {
        result = encodeSingle(result, separator);
      }
    }
    return Bytes.toBytes(result);
  }

  /**
   * Decode the given separators in the token with their decoding equivalents.
   * It <em>must</em> be used for the result encoded with
   * {@link #encode(String, Separator...)} with the same separators to be able
   * to recover the original.
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
   * Decode the given separators in the token with their decoding equivalents.
   * It <em>must</em> be used for the result encoded with
   * {@link #encode(String, Separator...)} with the same separators to be able
   * to recover the original.
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
        result = decodeSingle(result, separator);
      }
    }
    // decode percent to de-escape
    return decodePercent(result);
  }

  /**
   * Returns a single byte array containing all of the individual arrays
   * components separated by this separator.
   *
   * @param components Byte array components to be joined together.
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
      sb.append(this.value)
          .append(encode(items[i].toString()));
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
      for (String val : valuePattern.split(compoundValue)) {
        result.add(decode(val));
      }
    }
    return result;
  }

  /**
   * Splits the source array into multiple array segments using this separator,
   * up to a maximum of count items. This will naturally produce copied byte
   * arrays for each of the split segments.
   *
   * @param source to be split
   * @param limit on how many segments are supposed to be returned. A
   *          non-positive value indicates no limit on number of segments.
   * @return source split by this separator.
   */
  public byte[][] split(byte[] source, int limit) {
    return split(source, this.bytes, limit);
  }

  /**
   * Splits the source array into multiple array segments using this separator.
   * The sizes indicate the sizes of the relative components/segments.
   * In case one of the segments contains this separator before the specified
   * size is reached, the separator will be considered part of that segment and
   * we will continue till size is reached.
   * Variable length strings cannot contain this separator and are indiced with
   * a size of {@value #VARIABLE_SIZE}. Such strings are encoded for this
   * separator and decoded after the results from split is returned.
   *
   * @param source byte array to be split.
   * @param sizes sizes of relative components/segments.
   * @return source split by this separator as per the sizes specified..
   */
  public byte[][] split(byte[] source, int[] sizes) {
    return split(source, this.bytes, sizes);
  }

  /**
   * Splits the source array into multiple array segments using this separator,
   * as many times as splits are found. This will naturally produce copied byte
   * arrays for each of the split segments.
   *
   * @param source byte array to be split
   * @return source split by this separator.
   */
  public byte[][] split(byte[] source) {
    return split(source, this.bytes);
  }

  /**
   * Returns a list of ranges identifying [start, end) -- closed, open --
   * positions within the source byte array that would be split using the
   * separator byte array.
   * The sizes indicate the sizes of the relative components/segments.
   * In case one of the segments contains this separator before the specified
   * size is reached, the separator will be considered part of that segment and
   * we will continue till size is reached.
   * Variable length strings cannot contain this separator and are indiced with
   * a size of {@value #VARIABLE_SIZE}. Such strings are encoded for this
   * separator and decoded after the results from split is returned.
   *
   * @param source the source data
   * @param separator the separator pattern to look for
   * @param sizes indicate the sizes of the relative components/segments.
   * @return a list of ranges.
   */
  private static List<Range> splitRanges(byte[] source, byte[] separator,
      int[] sizes) {
    List<Range> segments = new ArrayList<Range>();
    if (source == null || separator == null) {
      return segments;
    }
    // VARIABLE_SIZE here indicates that there is no limit to number of segments
    // to return.
    int limit = VARIABLE_SIZE;
    if (sizes != null && sizes.length > 0) {
      limit = sizes.length;
    }
    int start = 0;
    int currentSegment = 0;
    itersource: for (int i = 0; i < source.length; i++) {
      for (int j = 0; j < separator.length; j++) {
        if (source[i + j] != separator[j]) {
          continue itersource;
        }
      }
      // all separator elements matched
      if (limit > VARIABLE_SIZE) {
        if (segments.size() >= (limit - 1)) {
          // everything else goes in one final segment
          break;
        }
        if (sizes != null) {
          int currentSegExpectedSize = sizes[currentSegment];
          if (currentSegExpectedSize > VARIABLE_SIZE) {
            int currentSegSize = i - start;
            if (currentSegSize < currentSegExpectedSize) {
              // Segment not yet complete. More bytes to parse.
              continue itersource;
            } else if (currentSegSize > currentSegExpectedSize) {
              // Segment is not as per size.
              throw new IllegalArgumentException(
                  "Segments not separated as per expected sizes");
            }
          }
        }
      }
      segments.add(new Range(start, i));
      start = i + separator.length;
      // i will be incremented again in outer for loop
      i += separator.length - 1;
      currentSegment++;
    }
    // add in remaining to a final range
    if (start <= source.length) {
      if (sizes != null) {
        // Check if final segment is as per size specified.
        if (sizes[currentSegment] > VARIABLE_SIZE &&
            source.length - start > sizes[currentSegment]) {
          // Segment is not as per size.
          throw new IllegalArgumentException(
              "Segments not separated as per expected sizes");
        }
      }
      segments.add(new Range(start, source.length));
    }
    return segments;
  }

  /**
   * Splits based on segments calculated based on limit/sizes specified for the
   * separator.
   *
   * @param source byte array to be split.
   * @param segments specifies the range for each segment.
   * @return a byte[][] split as per the segment ranges.
   */
  private static byte[][] split(byte[] source, List<Range> segments) {
    byte[][] splits = new byte[segments.size()][];
    for (int i = 0; i < segments.size(); i++) {
      Range r = segments.get(i);
      byte[] tmp = new byte[r.length()];
      if (tmp.length > 0) {
        System.arraycopy(source, r.start(), tmp, 0, r.length());
      }
      splits[i] = tmp;
    }
    return splits;
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator based on the sizes. This will naturally produce copied byte
   * arrays for each of the split segments.
   *
   * @param source source array.
   * @param separator separator represented as a byte array.
   * @param sizes sizes of relative components/segments.
   * @return byte[][] after splitting the source.
   */
  private static byte[][] split(byte[] source, byte[] separator, int[] sizes) {
    List<Range> segments = splitRanges(source, separator, sizes);
    return split(source, segments);
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator. This will naturally produce copied byte arrays for each of the
   * split segments.
   *
   * @param source Source array.
   * @param separator Separator represented as a byte array.
   * @return byte[][] after splitting the source.
   */
  private static byte[][] split(byte[] source, byte[] separator) {
    return split(source, separator, (int[]) null);
  }

  /**
   * Splits the source array into multiple array segments using the given
   * separator, up to a maximum of count items. This will naturally produce
   * copied byte arrays for each of the split segments.
   *
   * @param source Source array.
   * @param separator Separator represented as a byte array.
   * @param limit a non-positive value indicates no limit on number of segments.
   * @return byte[][] after splitting the input source.
   */
  private static byte[][] split(byte[] source, byte[] separator, int limit) {
    int[] sizes = null;
    if (limit > VARIABLE_SIZE) {
      sizes = new int[limit];
    }
    List<Range> segments = splitRanges(source, separator, sizes);
    return split(source, segments);
  }
}
