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

package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;

/**
  *  This partitioner rehashes values returned by {@link Object#hashCode()}
  *  to get smoother distribution between partitions which may improve
  *  reduce reduce time in some cases and should harm things in no cases.
  *  This partitioner is suggested with Integer and Long keys with simple
  *  patterns in their distributions.
  *  @since 2.0.3
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RehashPartitioner<K, V> extends Partitioner<K, V> {

  /** prime number seed for increasing hash quality */
  private static final int SEED = 1591267453;

  /** Rehash {@link Object#hashCode()} to partition. */
  public int getPartition(K key, V value, int numReduceTasks) {
    int h = SEED ^ key.hashCode();
    h ^= (h >>> 20) ^ (h >>> 12);
    h = h ^ (h >>> 7) ^ (h >>> 4);

    return (h & Integer.MAX_VALUE) % numReduceTasks;
  }
}
