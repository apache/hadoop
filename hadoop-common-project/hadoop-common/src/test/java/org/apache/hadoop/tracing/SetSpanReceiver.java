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

import com.google.common.base.Supplier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.HTraceConfiguration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;

/**
 * Span receiver that puts all spans into a single set.
 * This is useful for testing.
 * <p/>
 * We're not using HTrace's POJOReceiver here so as that doesn't
 * push all the metrics to a static place, and would make testing
 * SpanReceiverHost harder.
 */
public class SetSpanReceiver extends SpanReceiver {

  public SetSpanReceiver(HTraceConfiguration conf) {
  }

  public void receiveSpan(Span span) {
    SetHolder.spans.put(span.getSpanId(), span);
  }

  public void close() {
  }

  public static void clear() {
    SetHolder.spans.clear();
  }

  public static int size() {
    return SetHolder.spans.size();
  }

  public static Collection<Span> getSpans() {
    return SetHolder.spans.values();
  }

  public static Map<String, List<Span>> getMap() {
    return SetHolder.getMap();
  }

  public static class SetHolder {
    public static ConcurrentHashMap<SpanId, Span> spans =
        new ConcurrentHashMap<SpanId, Span>();

    public static Map<String, List<Span>> getMap() {
      Map<String, List<Span>> map = new HashMap<String, List<Span>>();

      for (Span s : spans.values()) {
        List<Span> l = map.get(s.getDescription());
        if (l == null) {
          l = new LinkedList<Span>();
          map.put(s.getDescription(), l);
        }
        l.add(s);
      }
      return map;
    }
  }

  public static void assertSpanNamesFound(final String[] expectedSpanNames) {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
          for (String spanName : expectedSpanNames) {
            if (!map.containsKey(spanName)) {
              return false;
            }
          }
          return true;
        }
      }, 100, 1000);
    } catch (TimeoutException e) {
      Assert.fail("timed out to get expected spans: " + e.getMessage());
    } catch (InterruptedException e) {
      Assert.fail("interrupted while waiting spans: " + e.getMessage());
    }
  }
}
