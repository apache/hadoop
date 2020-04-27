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
 * This package contains support for statistic collection and reporting.
 * This is the public API; implementation classes are to be kept elsewhere.
 *
 * This package is defines two interfaces
 *
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource}:
 * a source of statistic data, which can be retrieved
 * through a call to
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource#getIOStatistics()} .
 *
 * {@link org.apache.hadoop.fs.statistics.IOStatistics} the statistics retrieved from a statistics source.
 *
 * The retrieved statistics may be an immutable snapshot -in which case to get
 * updated statistics another call to
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource#getIOStatistics()}
 * must be made. Or they may be dynamic -in which case every time a specific
 * statistic is retrieved, the latest version is returned. Callers should assume
 * that if a statistics instance is dynamic, there is no atomicity when querying
 * multiple statistics. If the statistics source was a closeable object (e.g. a
 * stream), the statistics MUST remain valid after the stream is closed.
 *
 * Use pattern:
 *
 * An application probes an object (filesystem, stream etc) for implementation of
 * {@code IOStatisticsSource}, and, if it is, calls {@code getIOStatistics()}
 * to get its statistics.
 * If this is non-null, the client has statistics on the current
 * state of the statistics.
 * If dynamic, statistics can be enumerated and whenever
 * they are retrieved: the latest value will be returned.
 *
 * These statistics can be used to: log operations, profile applications, make
 * assertions about the state of the output.
 *
 * The names of statistics are a matter of choice of the specific source.
 * However, {@link org.apache.hadoop.fs.statistics.StoreStatisticNames}
 * contains a
 * set of names recommended for object store operations.
 * {@link org.apache.hadoop.fs.statistics.StreamStatisticNames} declares
 * recommended names for statistics provided for
 * input and output streams.
 *
 * They can also be serialized to build statistics on the overall cost of
 * operations, or printed to help diagnose performance/cost issues.
 *
 * Implementors notes
 * <ol>
 * <li>
 * IOStatistics keys SHOULD be standard names where possible.
 * </li>
 * <li>
 * MUST be unique to that specific instance of {@link IOStatisticsSource}.
 * </li>
 * <li>
 * MUST return the same values irrespective of which thread the statistics are
 * retrieved or its keys evaluated.
 * </li>
 * <li>
 * MUST NOT remove keys once a statistic instance has been created.
 * </li>
 * <li>
 * MUST NOT add keys once a statistic instance has been created.
 * </li>
 * <li>
 * MUST NOT block for long periods of time while blocking operations
 * (reads, writes) are taking place in the source.
 * That is: minimal synchronization points (AtomicLongs etc.) may be
 * used to share values, but retrieval of statistics should
 * be fast and return values even while slow/blocking remote IO is underway.
 * </li>
 * <li>
 * MUST support value enumeration and retrieval after the source has been closed.
 * </li>
 * <li>
 * SHOULD NOT have back-references to potentially expensive objects (filesystem
 * instances etc)
 * </li>
 * <li>
 * SHOULD provide statistics which can be added to generate aggregate statistics.
 * </li>
 * </ol>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
package org.apache.hadoop.fs.statistics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
