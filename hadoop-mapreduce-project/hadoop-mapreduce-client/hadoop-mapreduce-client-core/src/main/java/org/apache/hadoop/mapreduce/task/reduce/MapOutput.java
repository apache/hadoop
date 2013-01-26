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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.InputStream;
import java.io.IOException;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public abstract class MapOutput<K, V> {
  private static AtomicInteger ID = new AtomicInteger(0);
  
  private final int id;
  private final TaskAttemptID mapId;
  private final long size;
  private final boolean primaryMapOutput;
  
  public MapOutput(TaskAttemptID mapId, long size, boolean primaryMapOutput) {
    this.id = ID.incrementAndGet();
    this.mapId = mapId;
    this.size = size;
    this.primaryMapOutput = primaryMapOutput;
  }
  
  public boolean isPrimaryMapOutput() {
    return primaryMapOutput;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapOutput) {
      return id == ((MapOutput)obj).id;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id;
  }

  public TaskAttemptID getMapId() {
    return mapId;
  }

  public long getSize() {
    return size;
  }

  public abstract void shuffle(MapHost host, InputStream input,
                               long compressedLength,
                               long decompressedLength,
                               ShuffleClientMetrics metrics,
                               Reporter reporter) throws IOException;

  public abstract void commit() throws IOException;
  
  public abstract void abort();

  public abstract String getDescription();

  public String toString() {
    return "MapOutput(" + mapId + ", " + getDescription() + ")";
  }
  
  public static class MapOutputComparator<K, V> 
  implements Comparator<MapOutput<K, V>> {
    public int compare(MapOutput<K, V> o1, MapOutput<K, V> o2) {
      if (o1.id == o2.id) { 
        return 0;
      }
      
      if (o1.size < o2.size) {
        return -1;
      } else if (o1.size > o2.size) {
        return 1;
      }
      
      if (o1.id < o2.id) {
        return -1;
      } else {
        return 1;
      
      }
    }
  }
  
}
