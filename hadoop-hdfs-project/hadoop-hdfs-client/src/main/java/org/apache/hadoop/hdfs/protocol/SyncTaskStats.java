/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Each block sync task & meta sync task will feedback
 * {@link BlockSyncTaskExecutionFeedback} or
 * {@link MetadataSyncTaskExecutionFeedback} to NameNode.
 * The feedback will be parsed to a SyncTaskStats to indicate the amount
 * of bytes successfully or unsuccessfully tackled for a dispatched operation.
 */
public class SyncTaskStats {
  
  private static final String[] UNITS =
      new String[]{"B", "KiB", "MiB", "GiB", "TiB"};

  /**
   * Metrics for the syncservice.
   */
  public static class Metrics {
    private int ops;
    private long bytes;

    Metrics(int ops, long bytes) {
      this.ops = ops;
      this.bytes = bytes;
    }
    public static Metrics of(int ops, long bytes) {
      return new Metrics(ops, bytes);
    }

    public Metrics add(Metrics other) {
      this.ops += other.ops;
      this.bytes += other.bytes;
      return this;
    }

    public static Metrics empty() {
      return new Metrics(0, 0);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Metrics metrics = (Metrics) o;
      return ops == metrics.ops &&
          bytes == metrics.bytes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(ops, bytes);
    }

    @Override
    public String toString() {
      return "Metrics{ops=" + ops + ", bytes=" + bytes + "}";
    }
  }

  Map<MetadataSyncTaskOperation, Metrics> metaSuccesses = Maps.newHashMap();
  Map<MetadataSyncTaskOperation, Integer> metaFailures = Maps.newHashMap();

  Metrics blockSuccesses = Metrics.of(0, 0);
  Integer blockFailures = 0;

  private SyncTaskStats(){}

  @VisibleForTesting
  SyncTaskStats(boolean success, MetadataSyncTaskOperation operation,
      long numberOfBytes) {
    assert success;
    metaSuccesses.put(operation, Metrics.of(1, numberOfBytes));
  }

  @VisibleForTesting
  SyncTaskStats(boolean success, MetadataSyncTaskOperation operation) {
    assert !success;
    this.metaFailures.put(operation, 1);
  }


  @VisibleForTesting
  SyncTaskStats(boolean success, long numberOfBytes) {
    assert success;
    this.blockSuccesses.add(Metrics.of(1, numberOfBytes));
  }

  @VisibleForTesting
  SyncTaskStats(boolean success) {
    assert !success;
    blockFailures += 1;
  }

  @VisibleForTesting
  SyncTaskStats(Map<MetadataSyncTaskOperation, Metrics> metaSuccesses,
      Map<MetadataSyncTaskOperation, Integer> metaFailures,
      Metrics blockSuccesses, int blockFailures){
    this.metaSuccesses = metaSuccesses;
    this.blockSuccesses = blockSuccesses;
    this.metaFailures = metaFailures;
    this.blockFailures = blockFailures;
  }

  public static SyncTaskStats empty() {
    return new SyncTaskStats();
  }

  public static SyncTaskStats from(
      MetadataSyncTaskExecutionFeedback feedback) {
    if (SyncTaskExecutionOutcome.FINISHED_SUCCESSFULLY ==
        feedback.getOutcome()) {
      return new SyncTaskStats(true, feedback.getOperation(),
          feedback.getResult().getNumberOfBytes());
    } else {
      return new SyncTaskStats(false, feedback.getOperation());
    }
  }

  public static SyncTaskStats from(BlockSyncTaskExecutionFeedback feedback) {
    if (SyncTaskExecutionOutcome.FINISHED_SUCCESSFULLY ==
        feedback.getOutcome()) {
      return new SyncTaskStats(true,
          feedback.getResult().getNumberOfBytes());
    } else {
      return new SyncTaskStats(false);
    }
  }

  private static Metrics append(Metrics lhs, Metrics rhs) {
    return Metrics.of(lhs.ops + rhs.ops, lhs.bytes + rhs.bytes);
  }

  private static <K, V> Map<K, V> append(Map<K, V> left,
      Map<K, V> right, V defaultVal,
      BiFunction<V, V, V> appender) {
    Sets.SetView<K> keyUnion = Sets.union(left.keySet(), right.keySet());
    Map<K, V> freshMap = Maps.newHashMap();

    for (K key : keyUnion) {
      V leftTuple = left.getOrDefault(key, defaultVal);
      V rightTuple = right.getOrDefault(key, defaultVal);
      freshMap.put(key, appender.apply(leftTuple, rightTuple));
    }
    return freshMap;
  }

  public static SyncTaskStats append(SyncTaskStats left, SyncTaskStats right) {
    Map<MetadataSyncTaskOperation, Metrics> freshSuccesses =
        append(left.metaSuccesses, right.metaSuccesses,
            Metrics.empty(), SyncTaskStats::append);
    Metrics freshBlockSuccesses =
        append(left.blockSuccesses, right.blockSuccesses);
    Map<MetadataSyncTaskOperation, Integer> freshFailures =
        append(left.metaFailures, right.metaFailures,
            0, Integer::sum);
    int freshBlockFailures = left.blockFailures + right.blockFailures;
    return new SyncTaskStats(freshSuccesses, freshFailures,
        freshBlockSuccesses, freshBlockFailures);
  }

  public Map<MetadataSyncTaskOperation, Metrics> getMetaSuccesses() {
    return metaSuccesses;
  }

  public Map<MetadataSyncTaskOperation, Integer> getMetaFailures() {
    return metaFailures;
  }

  @VisibleForTesting
  public String prettyPrint() {
    StringBuilder builder = new StringBuilder();
    builder.append("Successes");
    builder.append("\n");
    builder.append("---------");
    builder.append("\n");
    Set<Map.Entry<MetadataSyncTaskOperation, Metrics>> successEntries =
        metaSuccesses.entrySet();
    prettyPrintSuccesses(builder, successEntries);
    builder.append("\n");
    builder.append("Failures");
    builder.append("\n");
    builder.append("--------");
    builder.append("\n");
    Set<Map.Entry<MetadataSyncTaskOperation, Integer>> failureEntries =
        metaFailures.entrySet();
    prettyPrintFailures(builder, failureEntries);
    return builder.toString();
  }

  private void prettyPrintSuccesses(StringBuilder builder,
      Set<Map.Entry<MetadataSyncTaskOperation, Metrics>> entries) {
    int numberOfCreates = entries
        .stream()
        .mapToInt(entry -> {
          MetadataSyncTaskOperation op = entry.getKey();
          if (op == MetadataSyncTaskOperation.MULTIPART_COMPLETE) {
            return entry.getValue().ops;
          } else {
            return 0;
          }
        })
        .sum();
    int numberOfRenames = entries
        .stream()
        .mapToInt(entry -> {
          MetadataSyncTaskOperation op = entry.getKey();
          if (op == MetadataSyncTaskOperation.RENAME_FILE) {
            return entry.getValue().ops;
          } else {
            return 0;
          }
        })
        .sum();
    int numberOfDeletes = entries
        .stream()
        .mapToInt(entry -> {
          MetadataSyncTaskOperation op = entry.getKey();
          if (op == MetadataSyncTaskOperation.DELETE_FILE) {
            return entry.getValue().ops;
          } else {
            return 0;
          }
        })
        .sum();

    long numberOfBytes = 0L;
    builder.append(String.format("%-30s= %s", "Files Created:",
        numberOfCreates));
    builder.append("\n");
    builder.append(String.format("%-30s= %s", "Files Renamed:",
        numberOfRenames));
    builder.append("\n");
    builder.append(String.format("%-30s= %s", "Files Deleted:",
        numberOfDeletes));
    builder.append("\n");
    builder.append(String.format("%-30s= %s", "Bytes Copied:",
        formatBytes(numberOfBytes)));
    builder.append("\n");
  }

  public String formatBytes(long bytes) {
    if (bytes == 0L) {
      return "0 Bytes";
    }
    int orderOfMagnitude = (int) (Math.log10(bytes) / Math.log10(1024));
    return new DecimalFormat("#,##0.#")
        .format(bytes / Math.pow(1024, orderOfMagnitude))
        + " " + UNITS[orderOfMagnitude];
  }

  private void prettyPrintFailures(StringBuilder builder,
       Set<Map.Entry<MetadataSyncTaskOperation, Integer>> entries) {
    long numberOfFailures = entries
        .stream()
        .mapToLong(Map.Entry::getValue)
        .sum();
    builder.append(String.format("%-30s= %s", "Total operations failed:",
        numberOfFailures));
    builder.append("\n");
  }

  public Metrics getBlockSuccesses() {
    return blockSuccesses;
  }

  public int getBlockFailures() {
    return blockFailures;
  }

}
