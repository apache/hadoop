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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The client-side metrics for hedged read feature.
 * This class has a number of metrics variables that are publicly accessible,
 * we can grab them from client side, like HBase.
 */
@InterfaceAudience.Private
public class DFSHedgedReadMetrics {
  public final AtomicLong hedgedReadOps = new AtomicLong();
  public final AtomicLong hedgedReadOpsWin = new AtomicLong();
  public final AtomicLong hedgedReadOpsInCurThread = new AtomicLong();

  public void incHedgedReadOps() {
    hedgedReadOps.incrementAndGet();
  }

  public void incHedgedReadOpsInCurThread() {
    hedgedReadOpsInCurThread.incrementAndGet();
  }

  public void incHedgedReadWins() {
    hedgedReadOpsWin.incrementAndGet();
  }

  public long getHedgedReadOps() {
    return hedgedReadOps.longValue();
  }

  public long getHedgedReadOpsInCurThread() {
    return hedgedReadOpsInCurThread.longValue();
  }

  public long getHedgedReadWins() {
    return hedgedReadOpsWin.longValue();
  }
}
