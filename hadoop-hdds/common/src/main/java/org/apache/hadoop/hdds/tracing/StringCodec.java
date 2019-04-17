/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.tracing;

import java.math.BigInteger;

import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.exceptions.EmptyTracerStateStringException;
import io.jaegertracing.internal.exceptions.MalformedTracerStateStringException;
import io.jaegertracing.internal.exceptions.TraceIdOutOfBoundException;
import io.jaegertracing.spi.Codec;
import io.opentracing.propagation.Format;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A jaeger codec to save the current tracing context as a string.
 */
public class StringCodec implements Codec<StringBuilder> {

  public static final Logger LOG  = LoggerFactory.getLogger(StringCodec.class);
  public static final StringFormat FORMAT = new StringFormat();

  @Override
  public JaegerSpanContext extract(StringBuilder s) {
    if (s == null) {
      throw new EmptyTracerStateStringException();
    }
    String value = s.toString();
    if (value != null && !value.equals("")) {
      String[] parts = value.split(":");
      if (parts.length != 4) {
        LOG.trace("MalformedTracerStateString: {}", value);
        throw new MalformedTracerStateStringException(value);
      } else {
        String traceId = parts[0];
        if (traceId.length() <= 32 && traceId.length() >= 1) {
          return new JaegerSpanContext(high(traceId),
              (new BigInteger(traceId, 16)).longValue(),
              (new BigInteger(parts[1], 16)).longValue(),
              (new BigInteger(parts[2], 16)).longValue(),
              (new BigInteger(parts[3], 16)).byteValue());
        } else {
          throw new TraceIdOutOfBoundException(
              "Trace id [" + traceId + "] length is not withing 1 and 32");
        }
      }
    } else {
      throw new EmptyTracerStateStringException();
    }
  }

  @Override
  public void inject(JaegerSpanContext context,
      StringBuilder string) {
    int intFlag = context.getFlags() & 255;
    string.append(
        context.getTraceId() + ":" + Long.toHexString(context.getSpanId())
            + ":" + Long.toHexString(context.getParentId()) + ":" + Integer
            .toHexString(intFlag));
  }

  private static long high(String hexString) {
    if (hexString.length() > 16) {
      int highLength = hexString.length() - 16;
      String highString = hexString.substring(0, highLength);
      return (new BigInteger(highString, 16)).longValue();
    } else {
      return 0L;
    }
  }

  /**
   * The format to save the context as text.
   * <p>
   * Using the mutable StringBuilder instead of plain String.
   */
  public static final class StringFormat implements Format<StringBuilder> {
  }

}
