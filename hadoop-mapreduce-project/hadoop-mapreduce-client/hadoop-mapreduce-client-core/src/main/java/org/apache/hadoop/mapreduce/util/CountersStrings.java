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

package org.apache.hadoop.mapreduce.util;

import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;

/**
 * String conversion utilities for counters.
 * Candidate for deprecation since we start to use JSON in 0.21+
 */
@InterfaceAudience.Private
public class CountersStrings {
  private static final char GROUP_OPEN = '{';
  private static final char GROUP_CLOSE = '}';
  private static final char COUNTER_OPEN = '[';
  private static final char COUNTER_CLOSE = ']';
  private static final char UNIT_OPEN = '(';
  private static final char UNIT_CLOSE = ')';
  private static char[] charsToEscape =  {GROUP_OPEN, GROUP_CLOSE,
                                          COUNTER_OPEN, COUNTER_CLOSE,
                                          UNIT_OPEN, UNIT_CLOSE};
  /**
   * Make the pre 0.21 counter string (for e.g. old job history files)
   * [(actual-name)(display-name)(value)]
   * @param counter to stringify
   * @return the stringified result
   */
  public static String toEscapedCompactString(Counter counter) {

    // First up, obtain the strings that need escaping. This will help us
    // determine the buffer length apriori.
    String escapedName, escapedDispName;
    long currentValue;
    synchronized(counter) {
      escapedName = escape(counter.getName());
      escapedDispName = escape(counter.getDisplayName());
      currentValue = counter.getValue();
    }
    int length = escapedName.length() + escapedDispName.length() + 4;


    length += 8; // For the following delimiting characters
    StringBuilder builder = new StringBuilder(length);
    builder.append(COUNTER_OPEN);

    // Add the counter name
    builder.append(UNIT_OPEN);
    builder.append(escapedName);
    builder.append(UNIT_CLOSE);

    // Add the display name
    builder.append(UNIT_OPEN);
    builder.append(escapedDispName);
    builder.append(UNIT_CLOSE);

    // Add the value
    builder.append(UNIT_OPEN);
    builder.append(currentValue);
    builder.append(UNIT_CLOSE);

    builder.append(COUNTER_CLOSE);

    return builder.toString();
  }

  /**
   * Make the 0.21 counter group string.
   * format: {(actual-name)(display-name)(value)[][][]}
   * where [] are compact strings for the counters within.
   * @param <G> type of the group
   * @param group to stringify
   * @return the stringified result
   */
  public static <G extends CounterGroupBase<?>>
  String toEscapedCompactString(G group) {
    List<String> escapedStrs = Lists.newArrayList();
    int length;
    String escapedName, escapedDispName;
    synchronized(group) {
      // First up, obtain the strings that need escaping. This will help us
      // determine the buffer length apriori.
      escapedName = escape(group.getName());
      escapedDispName = escape(group.getDisplayName());
      int i = 0;
      length = escapedName.length() + escapedDispName.length();
      for (Counter counter : group) {
        String escapedStr = toEscapedCompactString(counter);
        escapedStrs.add(escapedStr);
        length += escapedStr.length();
      }
    }
    length += 6; // for all the delimiting characters below
    StringBuilder builder = new StringBuilder(length);
    builder.append(GROUP_OPEN); // group start

    // Add the group name
    builder.append(UNIT_OPEN);
    builder.append(escapedName);
    builder.append(UNIT_CLOSE);

    // Add the display name
    builder.append(UNIT_OPEN);
    builder.append(escapedDispName);
    builder.append(UNIT_CLOSE);

    // write the value
    for(String escaped : escapedStrs) {
      builder.append(escaped);
    }

    builder.append(GROUP_CLOSE); // group end
    return builder.toString();
  }

  /**
   * Make the pre 0.21 counters string
   * @param <C> type of the counter
   * @param <G> type of the counter group
   * @param <T> type of the counters object
   * @param counters the object to stringify
   * @return the string in the following format
   * {(groupName)(group-displayName)[(counterName)(displayName)(value)]*}*
   */
  public static <C extends Counter, G extends CounterGroupBase<C>,
                 T extends AbstractCounters<C, G>>
  String toEscapedCompactString(T counters) {
    StringBuilder builder = new StringBuilder();
    synchronized(counters) {
      for (G group : counters) {
        builder.append(toEscapedCompactString(group));
      }
    }
    return builder.toString();
  }

  // Escapes all the delimiters for counters i.e {,[,(,),],}
  private static String escape(String string) {
    return StringUtils.escapeString(string, StringUtils.ESCAPE_CHAR,
                                    charsToEscape);
  }

  // Unescapes all the delimiters for counters i.e {,[,(,),],}
  private static String unescape(String string) {
    return StringUtils.unEscapeString(string, StringUtils.ESCAPE_CHAR,
                                      charsToEscape);
  }

  // Extracts a block (data enclosed within delimeters) ignoring escape
  // sequences. Throws ParseException if an incomplete block is found else
  // returns null.
  private static String getBlock(String str, char open, char close,
                                IntWritable index) throws ParseException {
    StringBuilder split = new StringBuilder();
    int next = StringUtils.findNext(str, open, StringUtils.ESCAPE_CHAR,
                                    index.get(), split);
    split.setLength(0); // clear the buffer
    if (next >= 0) {
      ++next; // move over '('

      next = StringUtils.findNext(str, close, StringUtils.ESCAPE_CHAR,
                                  next, split);
      if (next >= 0) {
        ++next; // move over ')'
        index.set(next);
        return split.toString(); // found a block
      } else {
        throw new ParseException("Unexpected end of block", next);
      }
    }
    return null; // found nothing
  }

  /**
   * Parse a pre 0.21 counters string into a counter object.
   * @param <C> type of the counter
   * @param <G> type of the counter group
   * @param <T> type of the counters object
   * @param compactString to parse
   * @param counters an empty counters object to hold the result
   * @return the counters object holding the result
   * @throws ParseException
   */
  @SuppressWarnings("deprecation")
  public static <C extends Counter, G extends CounterGroupBase<C>,
                 T extends AbstractCounters<C, G>>
  T parseEscapedCompactString(String compactString, T counters)
      throws ParseException {
    IntWritable index = new IntWritable(0);

    // Get the group to work on
    String groupString =
      getBlock(compactString, GROUP_OPEN, GROUP_CLOSE, index);

    while (groupString != null) {
      IntWritable groupIndex = new IntWritable(0);

      // Get the actual name
      String groupName =
          StringInterner.weakIntern(getBlock(groupString, UNIT_OPEN, UNIT_CLOSE, groupIndex));
      groupName = StringInterner.weakIntern(unescape(groupName));

      // Get the display name
      String groupDisplayName =
          StringInterner.weakIntern(getBlock(groupString, UNIT_OPEN, UNIT_CLOSE, groupIndex));
      groupDisplayName = StringInterner.weakIntern(unescape(groupDisplayName));

      // Get the counters
      G group = counters.getGroup(groupName);
      group.setDisplayName(groupDisplayName);

      String counterString =
        getBlock(groupString, COUNTER_OPEN, COUNTER_CLOSE, groupIndex);

      while (counterString != null) {
        IntWritable counterIndex = new IntWritable(0);

        // Get the actual name
        String counterName =
            StringInterner.weakIntern(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, counterIndex));
        counterName = StringInterner.weakIntern(unescape(counterName));

        // Get the display name
        String counterDisplayName =
            StringInterner.weakIntern(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE, counterIndex));
        counterDisplayName = StringInterner.weakIntern(unescape(counterDisplayName));

        // Get the value
        long value =
          Long.parseLong(getBlock(counterString, UNIT_OPEN, UNIT_CLOSE,
                                  counterIndex));

        // Add the counter
        Counter counter = group.findCounter(counterName);
        counter.setDisplayName(counterDisplayName);
        counter.increment(value);

        // Get the next counter
        counterString =
          getBlock(groupString, COUNTER_OPEN, COUNTER_CLOSE, groupIndex);
      }

      groupString = getBlock(compactString, GROUP_OPEN, GROUP_CLOSE, index);
    }
    return counters;
  }
}
