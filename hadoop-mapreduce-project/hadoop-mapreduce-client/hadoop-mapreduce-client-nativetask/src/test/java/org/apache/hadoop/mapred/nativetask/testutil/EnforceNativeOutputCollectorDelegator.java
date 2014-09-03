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
package org.apache.hadoop.mapred.nativetask.testutil;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator;

public class EnforceNativeOutputCollectorDelegator<K, V>
  extends NativeMapOutputCollectorDelegator<K, V> {
  private static final Log LOG = LogFactory.getLog(EnforceNativeOutputCollectorDelegator.class);
  private boolean nativetaskloaded = false;

  @Override
  public void init(Context context)
 throws IOException, ClassNotFoundException {
    try {
      super.init(context);
      nativetaskloaded = true;
    } catch (final Exception e) {
      nativetaskloaded = false;
      LOG.error("load nativetask lib failed, Native-Task Delegation is disabled", e);
    }
  }

  @Override
  public void collect(K key, V value, int partition) throws IOException, InterruptedException {
    if (this.nativetaskloaded) {
      super.collect(key, value, partition);
    } else {
      // nothing to do.
    }
  }
}
