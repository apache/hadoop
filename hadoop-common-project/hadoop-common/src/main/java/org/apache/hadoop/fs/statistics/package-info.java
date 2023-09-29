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
 * <p>
 * This package defines two interfaces:
 * <p>
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource}:
 * a source of statistic data, which can be retrieved
 * through a call to
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource#getIOStatistics()} .
 * <p>
 * {@link org.apache.hadoop.fs.statistics.IOStatistics} the statistics retrieved
 * from a statistics source.
 * <p>
 * The retrieved statistics may be an immutable snapshot -in which case to get
 * updated statistics another call to
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource#getIOStatistics()}
 * must be made. Or they may be dynamic -in which case every time a specific
 * statistic is retrieved, the latest version is returned. Callers should assume
 * that if a statistics instance is dynamic, there is no atomicity when querying
 * multiple statistics. If the statistics source was a closeable object (e.g. a
 * stream), the statistics MUST remain valid after the stream is closed.
 * <p>
 * Use pattern:
 * <p>
 * An application probes an object (filesystem, stream etc) to see if it
 * implements {@code IOStatisticsSource}, and, if it is,
 * calls {@code getIOStatistics()} to get its statistics.
 * If this is non-null, the client has statistics on the current
 * state of the statistics.
 * <p>
 * The expectation is that a statistics source is dynamic: when a value is
 * looked up the most recent values are returned.
 * When iterating through the set, the values of the iterator SHOULD
 * be frozen at the time the iterator was requested.
 * <p>
 * These statistics can be used to: log operations, profile applications,
 * and make assertions about the state of the output.
 * <p>
 * The names of statistics are a matter of choice of the specific source.
 * However, {@link org.apache.hadoop.fs.statistics.StoreStatisticNames}
 * contains a
 * set of names recommended for object store operations.
 * {@link org.apache.hadoop.fs.statistics.StreamStatisticNames} declares
 * recommended names for statistics provided for
 * input and output streams.
 * <p>
 * Utility classes:
 * <ul>
 *   <li>
 *     {@link org.apache.hadoop.fs.statistics.IOStatisticsSupport}.
 *     General support, including the ability to take a serializable
 *     snapshot of the current state of an IOStatistics instance.
 *   </li>
 *   <li>
 *     {@link org.apache.hadoop.fs.statistics.IOStatisticsLogging}.
 *     Methods for robust/on-demand string conversion, designed
 *     for use in logging statements and {@code toString()} implementations.
 *   </li>
 *   <li>
 *     {@link org.apache.hadoop.fs.statistics.IOStatisticsSnapshot}.
 *     A static snaphot of statistics which can be marshalled via
 *     java serialization or as JSON via jackson. It supports
 *     aggregation, so can be used to generate aggregate statistics.
 *   </li>
 * </ul>
 *
 * <p>
 * Implementors notes:
 * <ol>
 * <li>
 * IOStatistics keys SHOULD be standard names where possible.
 * </li>
 * <li>
 * An IOStatistics instance MUST be unique to that specific instance of
 * {@link org.apache.hadoop.fs.statistics.IOStatisticsSource}.
 * (i.e. not shared the way StorageStatistics are)
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
 * MUST support value enumeration and retrieval after the source has been
 * closed.
 * </li>
 * <li>
 * SHOULD NOT have back-references to potentially expensive objects
 * (filesystem instances etc.)
 * </li>
 * <li>
 * SHOULD provide statistics which can be added to generate aggregate
 * statistics.
 * </li>
 * </ol>
 */

@InterfaceAudience.Public
@InterfaceStability.Unstable
package org.apache.hadoop.fs.statistics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
