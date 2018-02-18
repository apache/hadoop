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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * To be used to wrap an actual {@link BufferedMutator} in a type safe manner.
 *
 * @param <T> The class referring to the table to be written to.
 */
public class TypedBufferedMutator<T extends BaseTable<T>> {

  private final BufferedMutator bufferedMutator;

  /**
   * @param bufferedMutator the mutator to be wrapped for delegation. Shall not
   *          be null.
   */
  public TypedBufferedMutator(BufferedMutator bufferedMutator) {
    this.bufferedMutator = bufferedMutator;
  }

  public TableName getName() {
    return bufferedMutator.getName();
  }

  public Configuration getConfiguration() {
    return bufferedMutator.getConfiguration();
  }

  public void mutate(Mutation mutation) throws IOException {
    bufferedMutator.mutate(mutation);
  }

  public void mutate(List<? extends Mutation> mutations) throws IOException {
    bufferedMutator.mutate(mutations);
  }

  public void close() throws IOException {
    bufferedMutator.close();
  }

  public void flush() throws IOException {
    bufferedMutator.flush();
  }

  public long getWriteBufferSize() {
    return bufferedMutator.getWriteBufferSize();
  }

}
