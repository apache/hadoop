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

package org.apache.hadoop.hdfs.server.datanode;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider.OPERATION;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link FileIoEvents} that simply counts the number of operations.
 * Not meant to be used outside of testing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CountingFileIoEvents extends FileIoEvents {
  private final Map<OPERATION, Counts> counts;

  private static class Counts {
    private final AtomicLong successes = new AtomicLong(0);
    private final AtomicLong failures = new AtomicLong(0);

    @JsonProperty("Successes")
    public long getSuccesses() {
      return successes.get();
    }

    @JsonProperty("Failures")
    public long getFailures() {
      return failures.get();
    }
  }

  public CountingFileIoEvents() {
    counts = new HashMap<>();
    for (OPERATION op : OPERATION.values()) {
      counts.put(op, new Counts());
    }
  }

  @Override
  public long beforeMetadataOp(
      @Nullable FsVolumeSpi volume, OPERATION op) {
    return 0;
  }

  @Override
  public void afterMetadataOp(
      @Nullable FsVolumeSpi volume, OPERATION op, long begin) {
    counts.get(op).successes.incrementAndGet();
  }

  @Override
  public long beforeFileIo(
      @Nullable FsVolumeSpi volume, OPERATION op, long len) {
    return 0;
  }

  @Override
  public void afterFileIo(
      @Nullable FsVolumeSpi volume, OPERATION op, long begin, long len) {
    counts.get(op).successes.incrementAndGet();
  }

  @Override
  public void onFailure(
      @Nullable FsVolumeSpi volume, OPERATION op, Exception e, long begin) {
    counts.get(op).failures.incrementAndGet();
  }

  @Override
  public String getStatistics() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(counts);
    } catch (JsonProcessingException e) {
      // Failed to serialize. Don't log the exception call stack.
      FileIoProvider.LOG.error("Failed to serialize statistics" + e);
      return null;
    }
  }
}