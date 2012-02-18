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
package org.apache.hadoop.tools.rumen;

import java.util.List;

import org.apache.hadoop.mapred.TaskStatus.State;

/**
 * {@link MapTaskAttemptInfo} represents the information with regard to a
 * map task attempt.
 */
public class MapTaskAttemptInfo extends TaskAttemptInfo {
  private long runtime;

  public MapTaskAttemptInfo(State state, TaskInfo taskInfo,
                            long runtime, List<List<Integer>> allSplits) {
    super(state, taskInfo,
          allSplits == null
            ? LoggedTaskAttempt.SplitVectorKind.getNullSplitsVector()
           : allSplits);
    this.runtime = runtime;
  }

  /**
   *
   * @deprecated please use the constructor with 
   *               {@code (state, taskInfo, runtime,
   *                  List<List<Integer>> allSplits)}
   *             instead.  
   *
   * see {@link LoggedTaskAttempt} for an explanation of
   *        {@code allSplits}.
   *
   * If there are no known splits, use {@code null}.
   */
  @Deprecated
  public MapTaskAttemptInfo(State state, TaskInfo taskInfo,
                            long runtime) {
    this(state, taskInfo, runtime, null);
  }

  @Override
  public long getRuntime() {
    return getMapRuntime();
  }

  /**
   * Get the runtime for the <b>map</b> phase of the map-task attempt.
   * 
   * @return the runtime for the <b>map</b> phase of the map-task attempt
   */
  public long getMapRuntime() {
    return runtime;
  }

}
