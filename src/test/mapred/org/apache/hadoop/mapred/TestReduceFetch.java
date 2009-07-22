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

package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.TaskCounter;

public class TestReduceFetch extends TestReduceFetchFromPartialMem {

  static {
    setSuite(TestReduceFetch.class);
  }

  /**
   * Verify that all segments are read from disk
   * @throws Exception might be thrown
   */
  public void testReduceFromDisk() throws Exception {
    final int MAP_TASKS = 8;
    JobConf job = mrCluster.createJobConf();
    job.set("mapred.job.reduce.input.buffer.percent", "0.0");
    job.setNumMapTasks(MAP_TASKS);
    job.set("mapred.child.java.opts", "-Xmx128m");
    job.setInt("mapred.job.reduce.total.mem.bytes", 128 << 20);
    job.set("mapred.job.shuffle.input.buffer.percent", "0.14");
    job.setInt("io.sort.factor", 2);
    job.setInt("mapred.inmem.merge.threshold", 4);
    Counters c = runJob(job);
    final long spill = c.findCounter(TaskCounter.SPILLED_RECORDS).getCounter();
    final long out = c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getCounter();
    assertTrue("Expected all records spilled during reduce (" + spill + ")",
        spill >= 2 * out); // all records spill at map, reduce
    assertTrue("Expected intermediate merges (" + spill + ")",
        spill >= 2 * out + (out / MAP_TASKS)); // some records hit twice
  }

  /**
   * Verify that no segment hits disk.
   * @throws Exception might be thrown
   */
  public void testReduceFromMem() throws Exception {
    final int MAP_TASKS = 3;
    JobConf job = mrCluster.createJobConf();
    job.set("mapred.job.reduce.input.buffer.percent", "1.0");
    job.set("mapred.job.shuffle.input.buffer.percent", "1.0");
    job.setInt("mapred.job.reduce.total.mem.bytes", 128 << 20);
    job.setNumMapTasks(MAP_TASKS);
    Counters c = runJob(job);
    final long spill = c.findCounter(TaskCounter.SPILLED_RECORDS).getCounter();
    final long out = c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getCounter();
    assertEquals("Spilled records: " + spill, out, spill); // no reduce spill
  }
}
