package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;

public class TestNNThroughputBenchmark extends TestCase {

  /**
   * This test runs all benchmarks defined in {@link NNThroughputBenchmark}.
   */
  public void testNNThroughput() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "localhost:" + 0);
    NameNode.format(conf);
    NNThroughputBenchmark.runBenchmark(conf, new String[] {"-op", "all"});
  }
}
