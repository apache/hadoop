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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

/**
 * Order of the destinations when we have multiple of them. When the resolver
 * of files to subclusters (FileSubclusterResolver) has multiple destinations,
 * this determines which location should be checked first.
 */
public enum DestinationOrder {
  HASH, // Follow consistent hashing in the first folder level
  LOCAL, // Local first
  RANDOM, // Random order
  HASH_ALL, // Follow consistent hashing
  SPACE // Available space based order
}