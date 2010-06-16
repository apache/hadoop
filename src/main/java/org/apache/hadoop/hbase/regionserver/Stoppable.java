/**
 * Copyright 2010 The Apache Software Foundation
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

/**
 * Implementations are stoppable.
 */
interface Stoppable {
  // Starting small, just doing a stoppable/stop for now and keeping it package
  // protected for now.  Needed so don't have to pass RegionServer instance
  // everywhere.  Doing Lifecycle seemed a stretch since none of our servers
  // do natural start/stop, etc. RegionServer is hosted in a Thread (can't do
  // 'stop' on a Thread and 'start' has special meaning for Threads) and then
  // Master is implemented differently again (it is a Thread itself). We
  // should move to redoing Master and RegionServer servers to use Spring or
  // some such container but for now, I just need stop -- St.Ack.
  /**
   * Stop service.
   */
  public void stop();
}
