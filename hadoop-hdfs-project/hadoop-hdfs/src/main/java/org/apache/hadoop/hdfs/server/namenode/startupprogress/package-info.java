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

/**
 * This package provides a mechanism for tracking {@link NameNode} startup
 * progress.  The package models NameNode startup as a series of {@link Phase}s,
 * with each phase further sub-divided into multiple {@link Step}s.  All phases
 * are coarse-grained and typically known in advance, implied by the structure of
 * the NameNode codebase (example: loading fsimage).  Steps are more granular and
 * typically only known at runtime after startup begins (example: loading a
 * specific fsimage file with a known length from a particular location).
 * 
 * {@link StartupProgress} provides a thread-safe data structure for
 * recording status information and counters.  Various parts of the NameNode
 * codebase use this to describe the NameNode's activities during startup.
 * 
 * {@link StartupProgressView} provides an immutable, consistent view of the
 * current state of NameNode startup progress.  This can be used to present the
 * data to a user.
 * 
 * {@link StartupProgressMetrics} exposes startup progress information via JMX
 * through the standard metrics system.
 */
@InterfaceAudience.Private
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
