/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io;

/**
 * Implementations can be asked for an estimate of their size in bytes.
 * <p>
 * Useful for sizing caches.  Its a given that implementation approximations
 * do not account for 32 vs 64 bit nor for different VM implementations.
 * <p>
 * An Object's size is determined by the non-static data members in it,
 * as well as the fixed {@link Object} overhead.
 * <p>
 * For example:
 * <pre>
 * public class SampleObject implements HeapSize {
 *   
 *   int [] numbers;
 *   int x;
 * }
 * </pre>
 */
public interface HeapSize {
  /**
   * @return Approximate 'exclusive deep size' of implementing object.  Includes
   * count of payload and hosting object sizings.
  */
  public long heapSize();
  
}
