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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;

/** 
 * Partitions the key space.
 * 
 * <p><code>Partitioner</code> controls the partitioning of the keys of the 
 * intermediate map-outputs. The key (or a subset of the key) is used to derive
 * the partition, typically by a hash function. The total number of partitions
 * is the same as the number of reduce tasks for the job. Hence this controls
 * which of the <code>m</code> reduce tasks the intermediate key (and hence the 
 * record) is sent for reduction.</p>
 *
 * <p>Note: A <code>Partitioner</code> is created only when there are multiple
 * reducers.</p>
 *
 * <p>Note: If you require your Partitioner class to obtain the Job's
 * configuration object, implement the {@link Configurable} interface.</p>
 *
 * Partitioner对map输出的key进行划分,决定key及其记录应该发往哪一个reducer.
 *
 * 即指定每一个key应该由哪个reducer来处理.
 *
 * 发往同一个reducer的所有key组成一个Partition.
 *
 * 如果作业只有一个reducer,则框架不会该作业创建Partitioner.
 *
 * 作业使用哪一个Partitioner由用户配置决定,Partitioner的逻辑中需要使用作业的配置信息,
 * 可以通过实现Configurable接口访问配置信息.
 * 
 * @see Reducer
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class Partitioner<KEY, VALUE> {
  
  /** 
   * Get the partition number for a given key (hence record) given the total 
   * number of partitions i.e. number of reduce-tasks for the job.
   *   
   * <p>Typically a hash function on a all or a subset of the key.</p>
   *
   * 每个Partition(Reducer)对应一个整数编号,该方法返回代码key所属的Partition
   * 的编号. 传入的Partition总数即为作业的reducer总数.
   *
   * @param key the key to be partioned.
   * @param value the entry value.
   * @param numPartitions the total number of partitions.
   * @return the partition number for the <code>key</code>.
   */
  public abstract int getPartition(KEY key, VALUE value, int numPartitions);
  
}
