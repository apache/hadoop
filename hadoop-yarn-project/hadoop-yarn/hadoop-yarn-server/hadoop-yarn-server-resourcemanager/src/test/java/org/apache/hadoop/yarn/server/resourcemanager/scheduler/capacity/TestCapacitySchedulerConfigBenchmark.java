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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeAttributeTestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Capacity Scheduler configuration benchmark: scheduler load, queue refresh
 * and configuration validation on generated trees of
 * {@link CSConfigBenchmarkGenerator}.
 *
 * <p>It only runs with {@code -DRunCapacitySchedulerConfigBenchmark=true}.
 * Each size should run in its own JVM, and each fork is a separate Maven
 * invocation, for example three forks of every size:</p>
 * <pre>
 * RM=hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager
 * for fork in 1 2 3; do for size in 10 100 1000 5000; do
 *   ./mvnw -o test -pl $RM -Dtest=TestCapacitySchedulerConfigBenchmark \
 *     -DRunCapacitySchedulerConfigBenchmark=true -Dcs.bench.sizes=$size \
 *     -Dcs.bench.fork=$fork -Dcs.bench.out=/tmp/cs-bench.txt
 * done; done
 * </pre>
 * <p>Properties:</p>
 * <ul>
 *   <li>{@code cs.bench.sizes}: requested queue counts, default
 *   {@code 10,100,1000,5000};</li>
 *   <li>{@code cs.bench.warmups} (default 2) and {@code cs.bench.iterations}
 *   (default 5) per operation; the median of the iterations is reported;</li>
 *   <li>{@code cs.bench.ops}: subset of {@code scheduler-load},
 *   {@code refresh} and {@code validate}, default all;</li>
 *   <li>{@code cs.bench.validators}: implementations measured by the
 *   {@code validate} operation, default {@code legacy}
 *   ({@link CapacitySchedulerConfigValidator#validateCSConfiguration}, the
 *   core of {@code POST /scheduler-conf/validate}); any other entry is the
 *   class name of a {@link ProposalValidator} with a no-argument constructor,
 *   which is the hook for {@code validate/v2};</li>
 *   <li>{@code cs.bench.capacityMode}: a
 *   {@link CSConfigBenchmarkGenerator.CapacityMode}, default
 *   {@code PCT_WEIGHT_MIXED};</li>
 *   <li>{@code cs.bench.fork}: label printed with the results;</li>
 *   <li>{@code cs.bench.out}: file the {@code BENCH} result lines are
 *   appended to (they are always printed to standard output).</li>
 * </ul>
 * <p>Operations: {@code scheduler-load} initializes a fresh
 * {@link CapacityScheduler} from the configuration, {@code refresh}
 * reinitializes the running scheduler with the unchanged configuration (what
 * {@code AdminService.refreshQueues} does after loading the file), and
 * {@code validate} validates a proposal that moves one percent of capacity
 * between two sibling leaves.</p>
 */
public class TestCapacitySchedulerConfigBenchmark {

  /** A validation implementation measured by the {@code validate} op. */
  public interface ProposalValidator {
    /**
     * Validates {@code proposed} (the live configuration with the proposal
     * applied) against the running scheduler, throwing when it is invalid.
     */
    void validate(RMContext rmContext, Configuration live,
        Configuration proposed) throws Exception;
  }

  /** The legacy {@code POST /scheduler-conf/validate} core. */
  public static final class LegacyValidator implements ProposalValidator {
    @Override
    public void validate(RMContext rmContext, Configuration live,
        Configuration proposed) throws IOException {
      CapacitySchedulerConfigValidator.validateCSConfiguration(live, proposed,
          rmContext);
    }
  }

  private interface Op {
    void run() throws Exception;
  }

  private final List<String> ops = Arrays.asList(System.getProperty(
      "cs.bench.ops", "scheduler-load,refresh,validate").split(","));
  private final int warmups = Integer.getInteger("cs.bench.warmups", 2);
  private final int iterations = Integer.getInteger("cs.bench.iterations", 5);
  private final String fork = System.getProperty("cs.bench.fork", "1");

  @Test
  public void testConfigurationOperations() throws Exception {
    assumeTrue(Boolean.getBoolean("RunCapacitySchedulerConfigBenchmark"));
    for (String size : System.getProperty("cs.bench.sizes", "10,100,1000,5000")
        .split(",")) {
      runForSize(Integer.parseInt(size.trim()));
    }
  }

  private void runForSize(int requested) throws Exception {
    CSConfigBenchmarkGenerator.GeneratedConfig generated =
        CSConfigBenchmarkGenerator.generate(requested,
            CSConfigBenchmarkGenerator.CapacityMode.valueOf(System.getProperty(
                "cs.bench.capacityMode", "PCT_WEIGHT_MIXED")));
    Map<String, String> proposal = toMap(
        CSConfigBenchmarkGenerator.createMutatedCopy(generated.getConf(), generated));
    YarnConfiguration conf =
        NodeAttributeTestUtils.getRandomDirConf(generated.getConf());
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      // MockRM switches the root logger to DEBUG, which dominates the timings.
      GenericTestUtils.setRootLogLevel(Level.WARN);
      rm.registerNode("h1:1234", 100 * 1024, 100);
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMContext rmContext = rm.getRMContext();
      int queues = cs.getCapacitySchedulerQueueManager().getQueues().size() - 1;
      assertEquals(generated.getQueueCount(), queues);

      measure(requested, queues, "scheduler-load", () -> {
        CapacityScheduler fresh = new CapacityScheduler();
        try {
          fresh.setConf(cs.getConf());
          fresh.setRMContext(rmContext);
          fresh.init(cs.getConf());
          assertEquals(queues + 1,
              fresh.getCapacitySchedulerQueueManager().getQueues().size());
        } finally {
          fresh.stop();
        }
      });
      measure(requested, queues, "refresh",
          () -> cs.reinitialize(cs.getConf(), rmContext));

      Configuration live = cs.getConf();
      Configuration proposed = new Configuration(live);
      proposal.forEach(proposed::set);
      for (String name : System.getProperty("cs.bench.validators", "legacy")
          .split(",")) {
        ProposalValidator validator = name.equals("legacy") ? new LegacyValidator()
            : (ProposalValidator) Class.forName(name.trim()).getDeclaredConstructor()
                .newInstance();
        measure(requested, queues, "validate", name.trim(),
            () -> validator.validate(rmContext, live, proposed));
      }
    } finally {
      rm.stop();
      QueueMetrics.clearQueueMetrics();
      GenericTestUtils.setRootLogLevel(Level.INFO);
    }
  }

  private void measure(int requested, int queues, String op, Op body)
      throws Exception {
    measure(requested, queues, op, null, body);
  }

  private void measure(int requested, int queues, String op, String variant,
      Op body) throws Exception {
    if (!ops.contains(op)) {
      return;
    }
    for (int i = 0; i < warmups; i++) {
      body.run();
    }
    double[] samples = new double[iterations];
    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      body.run();
      samples[i] = (System.nanoTime() - start) / 1e6;
    }
    double[] sorted = samples.clone();
    Arrays.sort(sorted);
    List<String> all = new ArrayList<>();
    for (double sample : samples) {
      all.add(String.format(Locale.ROOT, "%.1f", sample));
    }
    String line = String.format(Locale.ROOT,
        "BENCH fork=%s requested=%d queues=%d op=%s%s warmups=%d n=%d "
            + "median_ms=%.1f all_ms=%s",
        fork, requested, queues, op, variant == null ? "" : "-" + variant,
        warmups, iterations, sorted[(iterations - 1) / 2], String.join(",", all));
    System.out.println(line);
    String out = System.getProperty("cs.bench.out");
    if (out != null) {
      Files.write(new File(out).toPath(),
          (line + "\n").getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
  }

  private static Map<String, String> toMap(Configuration conf) {
    Map<String, String> map = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : conf) {
      map.put(e.getKey(), e.getValue());
    }
    return map;
  }
}
