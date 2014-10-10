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
package org.apache.hadoop.tracing;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.htrace.Sampler;
import org.htrace.impl.ProbabilitySampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class TraceSamplerFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(TraceSamplerFactory.class);

  public static Sampler<?> createSampler(Configuration conf) {
    String samplerStr = conf.get(CommonConfigurationKeys.HADOOP_TRACE_SAMPLER,
        CommonConfigurationKeys.HADOOP_TRACE_SAMPLER_DEFAULT);
    if (samplerStr.equals("NeverSampler")) {
      LOG.debug("HTrace is OFF for all spans.");
      return Sampler.NEVER;
    } else if (samplerStr.equals("AlwaysSampler")) {
      LOG.info("HTrace is ON for all spans.");
      return Sampler.ALWAYS;
    } else if (samplerStr.equals("ProbabilitySampler")) {
      double percentage =
          conf.getDouble("htrace.probability.sampler.percentage", 0.01d);
      LOG.info("HTrace is ON for " + percentage + "% of top-level spans.");
      return new ProbabilitySampler(percentage / 100.0d);
    } else {
      throw new RuntimeException("Can't create sampler " + samplerStr +
          ".  Available samplers are NeverSampler, AlwaysSampler, " +
          "and ProbabilitySampler.");
    }
  }
}
