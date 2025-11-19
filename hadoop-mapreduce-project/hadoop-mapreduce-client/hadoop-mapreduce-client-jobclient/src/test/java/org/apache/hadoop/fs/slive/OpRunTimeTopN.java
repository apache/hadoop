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

package org.apache.hadoop.fs.slive;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.slive.OperationOutput.OutputType;

/**
 * Utility types for tracking and encoding the Top-N longest operation runtimes.
 */
final class OpRunTimeTopN {

  static final int DEFAULT_LIMIT = 20;
  static final String ENTRY_DELIM = ";";
  static final String FIELD_DELIM = ",";
  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault());

  private OpRunTimeTopN() {
  }

  /**
   * Sample describing one operation execution.
   */
  static final class Sample implements Comparable<Sample> {
    final long startTime;
    final long endTime;
    final long duration;

    Sample(long startTime, long endTime) {
      this.startTime = Math.max(0L, startTime);
      this.endTime = Math.max(this.startTime, endTime);
      this.duration = this.endTime - this.startTime;
    }

    @Override
    public int compareTo(Sample other) {
      int cmp = Long.compare(this.duration, other.duration);
      if (cmp != 0) {
        return cmp;
      }
      // Preserve deterministic order by breaking ties on start time.
      return Long.compare(this.startTime, other.startTime);
    }
  }

  /**
   * Tracker used on the mapper side to keep only the longest samples.
   */
  static final class Tracker {
    private final int limit;
    private final Map<String, PriorityQueue<Sample>> byOperation;

    Tracker(int limit) {
      this.limit = limit;
      this.byOperation = new HashMap<String, PriorityQueue<Sample>>();
    }

    void record(String opType, long startTime, long endTime) {
      if (opType == null) {
        return;
      }
      PriorityQueue<Sample> queue = byOperation.get(opType);
      if (queue == null) {
        queue = new PriorityQueue<Sample>(limit);
        byOperation.put(opType, queue);
      }
      addSample(queue, new Sample(startTime, endTime), limit);
    }

    List<OperationOutput> buildOutputs() {
      List<OperationOutput> outputs =
          new ArrayList<OperationOutput>(byOperation.size());
      for (Map.Entry<String, PriorityQueue<Sample>> entry : byOperation
          .entrySet()) {
        String encoded = encodeSamples(entry.getValue());
        if (!encoded.isEmpty()) {
          outputs.add(new OperationOutput(OutputType.STRING, entry.getKey(),
              ReportWriter.OP_RUN_TIME, encoded));
        }
      }
      return outputs;
    }
  }

  /**
   * Aggregator used on the reducer side to compute the global Top-N.
   */
  static final class GlobalTopN {
    private final int limit;
    private final PriorityQueue<Sample> heap;

    GlobalTopN(int limit) {
      this.limit = limit;
      this.heap = new PriorityQueue<Sample>(limit);
    }

    void addEncodedSamples(String encoded) {
      if (encoded == null || encoded.isEmpty()) {
        return;
      }
      String[] entries = encoded.split(ENTRY_DELIM);
      for (String entry : entries) {
        if (entry == null) {
          continue;
        }
        entry = entry.trim();
        if (entry.isEmpty()) {
          continue;
        }
        String[] fields = entry.split(FIELD_DELIM);
        if (fields.length != 3) {
          throw new IllegalArgumentException(
              "Invalid runtime entry formatted as \"" + entry + '"');
        }
        try {
          long startTime = parseTimestamp(fields[0].trim());
          long endTime = parseTimestamp(fields[1].trim());
          addSample(heap, new Sample(startTime, endTime), limit);
        } catch (NumberFormatException | DateTimeParseException nfe) {
          throw new IllegalArgumentException("Invalid numeric value in entry \""
              + entry + '"', nfe);
        }
      }
    }

    String encode() {
      return encodeSamples(heap);
    }
  }

  private static void addSample(PriorityQueue<Sample> queue, Sample sample,
      int limit) {
    queue.add(sample);
    if (queue.size() > limit) {
      queue.poll();
    }
  }

  private static String encodeSamples(Collection<Sample> samples) {
    if (samples == null || samples.isEmpty()) {
      return "";
    }
    List<Sample> sorted = new ArrayList<Sample>(samples);
    Collections.sort(sorted, Collections.reverseOrder());
    StringBuilder builder = new StringBuilder();
    for (Sample sample : sorted) {
      if (builder.length() > 0) {
        builder.append(ENTRY_DELIM);
      }
      builder.append(formatTimestamp(sample.startTime));
      builder.append(FIELD_DELIM);
      builder.append(formatTimestamp(sample.endTime));
      builder.append(FIELD_DELIM);
      builder.append(sample.duration);
    }
    return builder.toString();
  }

  static String encodeSingle(long startTime, long endTime) {
    return encodeSamples(Collections.singletonList(new Sample(startTime, endTime)));
  }

  private static String formatTimestamp(long epochMillis) {
    return TIME_FORMATTER.format(Instant.ofEpochMilli(epochMillis));
  }

  private static long parseTimestamp(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return Instant.from(TIME_FORMATTER.parse(value)).toEpochMilli();
    }
  }
}

