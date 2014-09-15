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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.nativetask.serde.INativeSerializer;
import org.apache.hadoop.mapred.nativetask.serde.NativeSerialization;

/**
 * Base class for platforms. A platform is a framework running on top of
 * MapReduce, like Hadoop, Hive, Pig, Mahout. Each framework defines its
 * own key type and value type across a MapReduce job. For each platform,
 * we should implement serializers such that we could communicate data with
 * native side and native comparators so our native output collectors could
 * sort them and write out. We've already provided the {@link HadoopPlatform}
 * that supports all key types of Hadoop and users could implement their custom
 * platform.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Platform {
  private final NativeSerialization serialization;
  protected Set<String> keyClassNames = new HashSet<String>();

  public Platform() {
    this.serialization = NativeSerialization.getInstance();
  }

  /**
   * initialize a platform, where we should call registerKey
   */
  public abstract void init() throws IOException;

  /**
   * @return name of a Platform, useful for logs and debug
   */
  public abstract String name();


  /**
   * associate a key class with its serializer and platform
   *
   * @param keyClassName map out key class name
   * @param key          key serializer class
   */
  protected void registerKey(String keyClassName, Class<?> key) throws IOException {
    serialization.register(keyClassName, key);
    keyClassNames.add(keyClassName);
  }

  /**
   * whether a platform supports a specific key should at least satisfy two conditions
   *
   * 1. the key belongs to the platform
   * 2. the associated serializer must implement {@link INativeComparable} interface
   *
   *
   * @param keyClassName map out put key class name
   * @param serializer   serializer associated with key via registerKey
   * @param job          job configuration
   * @return             true if the platform has implemented native comparators of the key and
   *                     false otherwise
   */
  protected abstract boolean support(String keyClassName,
      INativeSerializer<?> serializer, JobConf job);


  /**
   * whether it's the platform that has defined a custom Java comparator
   *
   * NativeTask doesn't support custom Java comparators
   * (set with mapreduce.job.output.key.comparator.class)
   * but a platform (e.g Pig) could also set that conf and implement native
   * comparators so we shouldn't bail out.
   *
   * @param keyComparator comparator set with mapreduce.job.output.key.comparator.class
   */
  protected abstract boolean define(Class<?> keyComparator);
}
