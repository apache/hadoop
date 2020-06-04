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

package org.apache.hadoop.fs.statistics;

/**
 * Entry of an IOStatistic.
 * <p></p>
 * it's a pair of (type, values[]) such that it is
 * trivially serializable and we can add new types
 * with multiple entries.
 * <p></p>
 * For example, the data for a mean value would be
 * (mean, sample count);
 * for a maximum it would just be the max value.
 * What is key is that each entry MUST provide all
 * the data needed to aggregate two entries together.
 * <p></p>
 * This isn't perfect, as we'd really want a union of types,
 * so doubles could be passed round too.
 */
public class IOStatisticEntry {
  int type;
  long[] values;
}
