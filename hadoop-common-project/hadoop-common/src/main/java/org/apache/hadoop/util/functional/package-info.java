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
 * Support for functional programming within the Hadoop APIs.
 * <p></p>
 * Much of this is needed simply to cope with Java's checked exceptions and
 * the fact that the java.util.function can only throw runtime exceptions.
 * <p></p>
 * Pretty much all the Hadoop FS APIs raise IOExceptions, hence the need
 * for these classes. If Java had made a different decision about the
 * nature of exceptions, life would be better.
 * <p></p>
 * Do note that the {@link org.apache.hadoop.util.functional.RemoteIterators}
 * iterators go beyond that of the java ones, in terms of declaring themselves
 * Closeable and implementors of
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource}; a chain
 * of wrapped iterators can supply statistics of the inner iterators, and
 * encourage close() to be called after use.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
package org.apache.hadoop.util.functional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;