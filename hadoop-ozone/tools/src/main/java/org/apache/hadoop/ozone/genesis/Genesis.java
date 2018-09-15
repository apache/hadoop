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

import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Main class that executes a set of HDDS/Ozone benchmarks.
 * We purposefully don't use the runner and tools classes from Hadoop.
 * There are some name collisions with OpenJDK JMH package.
 * <p>
 * Hence, these classes do not use the Tool/Runner pattern of standard Hadoop
 * CLI.
 */
public final class Genesis {

  private Genesis() {
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchMarkContainerStateMap.class.getSimpleName())
//        .include(BenchMarkMetadataStoreReads.class.getSimpleName())
//        .include(BenchMarkMetadataStoreWrites.class.getSimpleName())
//        .include(BenchMarkDatanodeDispatcher.class.getSimpleName())
// Commenting this test out, till we support either a command line or a config
        // file based ability to run tests.
//        .include(BenchMarkRocksDbStore.class.getSimpleName())
        .warmupIterations(5)
        .measurementIterations(20)
        .addProfiler(StackProfiler.class)
        .shouldDoGC(true)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}


