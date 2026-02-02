/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This package contains classes that facilitate asynchronous operations within the Hadoop
 * Distributed File System (HDFS) Federation router. These classes are designed to work with
 * the Hadoop ecosystem, providing utilities and interfaces to perform non-blocking tasks that
 * can improve the performance and responsiveness of HDFS operations.
 *
 * <p>These classes work together to enable complex asynchronous workflows, making it easier to
 * write code that can handle long-running tasks without blocking, thus improving the overall
 * efficiency and scalability of HDFS operations.</p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

package org.apache.hadoop.hdfs.server.federation.router.async.utils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
