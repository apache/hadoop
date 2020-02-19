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

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.jaegertracing.internal.samplers.ConstSampler;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.htrace.core.HTraceConfiguration;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utility functions for tracing.
 */
@InterfaceAudience.Private
public class TraceUtils {
  private static List<ConfigurationPair> EMPTY = Collections.emptyList();
  static final String DEFAULT_HADOOP_TRACE_PREFIX = "hadoop.htrace.";

  static final Logger LOG = LoggerFactory.getLogger(TraceUtils.class);

  public static TraceConfiguration wrapHadoopConfOT(final String prefix,
      final Configuration conf) {
    // Do nothing for now. Might be useful for future config.
    return null;
  }

  public static HTraceConfiguration wrapHadoopConf(final String prefix,
        final Configuration conf) {
    return wrapHadoopConf(prefix, conf, EMPTY);
  }

  public static HTraceConfiguration wrapHadoopConf(final String prefix,
        final Configuration conf, List<ConfigurationPair> extraConfig) {
    final HashMap<String, String> extraMap = new HashMap<String, String>();
    for (ConfigurationPair pair : extraConfig) {
      extraMap.put(pair.getKey(), pair.getValue());
    }
    return new HTraceConfiguration() {
      @Override
      public String get(String key) {
        String ret = getInternal(prefix + key);
        if (ret != null) {
          return ret;
        }
        return getInternal(DEFAULT_HADOOP_TRACE_PREFIX  + key);
      }

      @Override
      public String get(String key, String defaultValue) {
        String ret = get(key);
        if (ret != null) {
          return ret;
        }
        return defaultValue;
      }

      private String getInternal(String key) {
        if (extraMap.containsKey(key)) {
          return extraMap.get(key);
        }
        return conf.get(key);
      }
    };
  }

  public static Tracer createAndRegisterTracer(String name) {
    if (!GlobalTracer.isRegistered()) {
      io.jaegertracing.Configuration config =
          io.jaegertracing.Configuration.fromEnv(name);
      Tracer tracer = config.getTracerBuilder().build();
      GlobalTracer.register(tracer);
    }

    return GlobalTracer.get();
  }

  public static SpanContext byteStringToSpanContext(ByteString byteString) {
    if (byteString == null || byteString.isEmpty()) {
      LOG.debug("The provided serialized context was null or empty");
      return null;
    }

    SpanContext context = null;
    ByteArrayInputStream stream =
        new ByteArrayInputStream(byteString.toByteArray());

    try {
      ObjectInputStream objStream = new ObjectInputStream(stream);
      Map<String, String> carrier =
          (Map<String, String>) objStream.readObject();

      context = GlobalTracer.get().extract(Format.Builtin.TEXT_MAP,
          new TextMapExtractAdapter(carrier));
    } catch (Exception e) {
      LOG.warn("Could not deserialize context {}", e);
    }

    return context;
  }

  public static ByteString spanContextToByteString(SpanContext context) {
    if (context == null) {
      LOG.debug("No SpanContext was provided");
      return null;
    }

    Map<String, String> carrier = new HashMap<String, String>();
    GlobalTracer.get().inject(context, Format.Builtin.TEXT_MAP,
        new TextMapInjectAdapter(carrier));
    if (carrier.isEmpty()) {
      LOG.warn("SpanContext was not properly injected by the Tracer.");
      return null;
    }

    ByteString byteString = null;
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try {
      ObjectOutputStream objStream = new ObjectOutputStream(stream);
      objStream.writeObject(carrier);
      objStream.flush();

      byteString = ByteString.copyFrom(stream.toByteArray());
      LOG.debug("SpanContext serialized, resulting byte length is {}",
          byteString.size());
    } catch (IOException e) {
      LOG.warn("Could not serialize context {}", e);
    }

    return byteString;
  }
}
