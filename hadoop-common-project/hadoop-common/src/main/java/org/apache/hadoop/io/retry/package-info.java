/*
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

/**
 * A mechanism for selectively retrying methods that throw exceptions under
 * certain circumstances.
 * Typical usage is
 *  UnreliableImplementation unreliableImpl = new UnreliableImplementation();
 *  UnreliableInterface unreliable = (UnreliableInterface)
 *  RetryProxy.create(UnreliableInterface.class, unreliableImpl,
 *  RetryPolicies.retryUpToMaximumCountWithFixedSleep(4, 10,
 *      TimeUnit.SECONDS));
 *  unreliable.call();
 *
 * This will retry any method called on <code>unreliable</code> four times -
 * in this case the <code>call()</code> method - sleeping 10 seconds between
 * each retry. There are a number of
 * {@link org.apache.hadoop.io.retry.RetryPolicies retry policies}
 * available, or you can implement a custom one by implementing
 * {@link org.apache.hadoop.io.retry.RetryPolicy}.
 * It is also possible to specify retry policies on a
 * {@link org.apache.hadoop.io.retry.RetryProxy#create(Class, Object, Map)
 * per-method basis}.
 */
@InterfaceAudience.LimitedPrivate({"HBase", "HDFS", "MapReduce"})
@InterfaceStability.Evolving
package org.apache.hadoop.io.retry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
