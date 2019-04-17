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
import org.openjdk.jmh.runner.options.OptionsBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Command;

/**
 * Main class that executes a set of HDDS/Ozone benchmarks.
 * We purposefully don't use the runner and tools classes from Hadoop.
 * There are some name collisions with OpenJDK JMH package.
 * <p>
 * Hence, these classes do not use the Tool/Runner pattern of standard Hadoop
 * CLI.
 */
@Command(name = "ozone genesis",
    description = "Tool for running ozone benchmarks",
    mixinStandardHelpOptions = true)
public final class Genesis {

  // After adding benchmark in genesis package add the benchmark name in the
  // description for this option.
  @Option(names = "-benchmark", split = ",", description =
      "Option used for specifying benchmarks to run.\n"
          + "Ex. ozone genesis -benchmark BenchMarkContainerStateMap,"
          + "BenchMarkOMKeyAllocation.\n"
          + "Possible benchmarks which can be used are "
          + "{BenchMarkContainerStateMap, BenchMarkOMKeyAllocation, "
          + "BenchMarkOzoneManager, BenchMarkOMClient, "
          + "BenchMarkSCM, BenchMarkMetadataStoreReads, "
          + "BenchMarkMetadataStoreWrites, BenchMarkDatanodeDispatcher, "
          + "BenchMarkRocksDbStore}")
  private static String[] benchmarks;

  @Option(names = "-t", defaultValue = "4",
      description = "Number of threads to use for the benchmark.\n"
          + "This option can be overridden by threads mentioned in benchmark.")
  private static int numThreads;

  private Genesis() {
  }

  public static void main(String[] args) throws RunnerException {
    CommandLine commandLine = new CommandLine(new Genesis());
    commandLine.parse(args);
    if (commandLine.isUsageHelpRequested()) {
      commandLine.usage(System.out);
      return;
    }

    OptionsBuilder optionsBuilder = new OptionsBuilder();
    if (benchmarks != null) {
      // The OptionsBuilder#include takes a regular expression as argument.
      // Therefore it is important to keep the benchmark names unique for
      // running a benchmark. For example if there are two benchmarks -
      // BenchMarkOM and BenchMarkOMClient and we include BenchMarkOM then
      // both the benchmarks will be run.
      for (String benchmark : benchmarks) {
        optionsBuilder.include(benchmark);
      }
    }
    optionsBuilder.warmupIterations(2)
        .measurementIterations(20)
        .addProfiler(StackProfiler.class)
        .shouldDoGC(true)
        .forks(1)
        .threads(numThreads);

    new Runner(optionsBuilder.build()).run();
  }
}


