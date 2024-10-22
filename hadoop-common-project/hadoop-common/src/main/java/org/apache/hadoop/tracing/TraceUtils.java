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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This class provides utility functions for tracing.
 */
@InterfaceAudience.Private
public class TraceUtils {
  public static final Logger LOG = LoggerFactory.getLogger(TraceUtils.class.getName());
  static final String DEFAULT_HADOOP_TRACE_PREFIX = "hadoop.htrace.";

  public static TraceConfiguration wrapHadoopConf(final String prefix, final Configuration conf) {
    return null;
  }

  public static Tracer createAndRegisterTracer(String name) {
    return null;
  }

  public static SpanContext mapToSpanContext(Map<String, String> kvMap) {
    return SpanContext.buildFromKVMap(kvMap);
  }

  public static Map<String, String> spanContextToMap(SpanContext context) {
    if (context == null) {
      return null;
    }
    return context.getKVSpanContext();
  }
}
