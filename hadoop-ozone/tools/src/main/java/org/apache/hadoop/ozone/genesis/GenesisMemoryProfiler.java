/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.genesis;

import org.apache.hadoop.conf.StorageUnit;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Max memory profiler.
 */
public class GenesisMemoryProfiler implements InternalProfiler {
  @Override
  public void beforeIteration(BenchmarkParams benchmarkParams,
      IterationParams iterationParams) {

  }

  @Override
  public Collection<? extends Result> afterIteration(BenchmarkParams
      benchmarkParams, IterationParams iterationParams, IterationResult
      result) {
    long totalHeap = Runtime.getRuntime().totalMemory();

    Collection<ScalarResult> samples = new ArrayList<>();
    samples.add(new ScalarResult("Max heap",
        StorageUnit.BYTES.toGBs(totalHeap), "GBs",
        AggregationPolicy.MAX));
    return samples;
  }

  @Override
  public String getDescription() {
    return "Genesis Memory Profiler. Computes Max Memory used by a test.";
  }
}

