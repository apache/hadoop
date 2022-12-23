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
package org.apache.hadoop.yarn.nodelabels.store;

import java.io.IOException;

/**
 * Define the interface for store activity.
 * Used by for FileSystem based operation.
 *
 * @param <W> write to be done to
 * @param <R> read to be done from
 * @param <M> manager used
 */
public interface StoreOp<W, R, M> {

  /**
   * Write operation to persistent storage.
   *
   * @param write write to be done to
   * @param mgr manager used by store
   * @throws IOException
   */
  void write(W write, M mgr) throws IOException;

  /**
   * Read and populate StoreOp.
   *
   * @param read read to be done from
   * @param mgr  manager used by store
   * @throws IOException
   */
  void recover(R read, M mgr) throws IOException;
}