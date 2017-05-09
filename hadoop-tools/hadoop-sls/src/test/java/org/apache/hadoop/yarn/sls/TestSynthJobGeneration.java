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
package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.yarn.sls.synthetic.SynthJob;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Simple test class driving the {@code SynthTraceJobProducer}, and validating
 * jobs produce are within expected range.
 */
public class TestSynthJobGeneration {

  public final static Logger LOG =
      Logger.getLogger(TestSynthJobGeneration.class);

  @Test
  public void test() throws IllegalArgumentException, IOException {

    Configuration conf = new Configuration();

    conf.set(SynthTraceJobProducer.SLS_SYNTHETIC_TRACE_FILE,
        "src/test/resources/syn.json");

    SynthTraceJobProducer stjp = new SynthTraceJobProducer(conf);

    SynthJob js = (SynthJob) stjp.getNextJob();

    int jobCount = 0;

    while (js != null) {
      LOG.info((jobCount++) + " " + js.getQueueName() + " -- "
          + js.getJobClass().getClassName() + " (conf: "
          + js.getJobConf().get(MRJobConfig.QUEUE_NAME) + ") " + " submission: "
          + js.getSubmissionTime() + ", " + " duration: " + js.getDuration()
          + " numMaps: " + js.getNumberMaps() + " numReduces: "
          + js.getNumberReduces());

      validateJob(js);
      js = (SynthJob) stjp.getNextJob();
    }

    Assert.assertEquals(stjp.getNumJobs(), jobCount);
  }

  private void validateJob(SynthJob js) {

    assertTrue(js.getSubmissionTime() > 0);
    assertTrue(js.getDuration() > 0);
    assertTrue(js.getNumberMaps() >= 0);
    assertTrue(js.getNumberReduces() >= 0);
    assertTrue(js.getNumberMaps() + js.getNumberReduces() > 0);
    assertTrue(js.getTotalSlotTime() >= 0);

    for (int i = 0; i < js.getNumberMaps(); i++) {
      TaskAttemptInfo tai = js.getTaskAttemptInfo(TaskType.MAP, i, 0);
      assertTrue(tai.getRuntime() > 0);
    }

    for (int i = 0; i < js.getNumberReduces(); i++) {
      TaskAttemptInfo tai = js.getTaskAttemptInfo(TaskType.REDUCE, i, 0);
      assertTrue(tai.getRuntime() > 0);
    }

    if (js.hasDeadline()) {
      assertTrue(js.getDeadline() > js.getSubmissionTime() + js.getDuration());
    }

  }
}
