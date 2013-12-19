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

package org.apache.hadoop.yarn.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;

@InterfaceAudience.Private
public interface RMFailoverProxyProvider<T> extends FailoverProxyProvider <T> {
  /**
   * Initialize internal data structures, invoked right after instantiation.
   *
   * @param conf Configuration to use
   * @param proxy The {@link RMProxy} instance to use
   * @param protocol The communication protocol to use
   */
  public void init(Configuration conf, RMProxy<T> proxy, Class<T> protocol);
}
