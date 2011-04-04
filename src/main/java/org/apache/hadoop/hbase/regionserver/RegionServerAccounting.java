/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicLong;

/**
 * RegionServerAccounting keeps record of some basic real time information about
 * the Region Server. Currently, it only keeps record the global memstore size. 
 */
public class RegionServerAccounting {

  private final AtomicLong atomicGlobalMemstoreSize = new AtomicLong(0);
  
  /**
   * @return the global Memstore size in the RegionServer
   */
  public long getGlobalMemstoreSize() {
    return atomicGlobalMemstoreSize.get();
  }
  
  /**
   * @param memStoreSize the Memstore size will be added to 
   *        the global Memstore size 
   * @return the global Memstore size in the RegionServer 
   */
  public long addAndGetGlobalMemstoreSize(long memStoreSize) {
    return atomicGlobalMemstoreSize.addAndGet(memStoreSize);
  }
 
}
