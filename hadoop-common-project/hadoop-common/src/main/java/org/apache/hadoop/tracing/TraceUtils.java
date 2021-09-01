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
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides utility functions for tracing.
 */
@InterfaceAudience.Private
public class TraceUtils {
  public static final Logger LOG = LoggerFactory.getLogger(TraceUtils.class.getName());
  static final String DEFAULT_HADOOP_TRACE_PREFIX = "hadoop.htrace.";

  public static TraceConfiguration wrapHadoopConf(final String prefix,
      final Configuration conf) {
    return null;
  }

  public static Tracer createAndRegisterTracer(String name) {
    return null;
  }

  public static SpanContext byteStringToSpanContext(ByteString byteString) {
    return deserialize(byteString);
  }

  public static ByteString spanContextToByteString(SpanContext context) {
    Map<String, String> kvMap = context.getKVSpanContext();
    ByteString byteString = serialize(kvMap);
    return byteString;
  }

  //Added this for tracing will remove this after having
  // a discussion
  static ByteString serialize(Object obj){
    try{
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream(out);
      os.writeObject(obj);
      os.flush();
      byte[] byteArray = out.toByteArray();
      return ByteString.copyFrom(byteArray);
    } catch (Exception e){
      LOG.error("Error in searializing the object:", e);
      return null;
    }
  }

  static SpanContext deserialize(ByteString spanContextByteString) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(spanContextByteString.toByteArray());
      ObjectInputStream is = new ObjectInputStream(in);
      Map<String, String> kvMap = (Map<String, String>) is.readObject();
      return SpanContext.buildFromKVMap(kvMap);
    } catch (Exception e) {
      LOG.error("Error in deserializing the object:", e);
      return null;
    }
  }
}
