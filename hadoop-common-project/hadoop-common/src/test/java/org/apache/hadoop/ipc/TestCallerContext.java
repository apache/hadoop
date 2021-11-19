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
package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_KEY;

public class TestCallerContext {
  @Test
  public void testBuilderAppend() {
    Configuration conf = new Configuration();
    conf.set(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY, "$");
    CallerContext.Builder builder = new CallerContext.Builder(null, conf);
    CallerContext context = builder.append("context1")
        .append("context2").append("key3", "value3").build();
    Assert.assertEquals(true,
        context.getContext().contains("$"));
    String[] items = context.getContext().split("\\$");
    Assert.assertEquals(3, items.length);
    Assert.assertEquals("key3:value3", items[2]);

    builder.append("$$");
    Assert.assertEquals("context1$context2$key3:value3$$$",
        builder.build().getContext());
  }

  @Test
  public void testBuilderAppendIfAbsent() {
    Configuration conf = new Configuration();
    conf.set(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY, "$");
    CallerContext.Builder builder = new CallerContext.Builder(null, conf);
    builder.append("key1", "value1");
    Assert.assertEquals("key1:value1",
        builder.build().getContext());

    // Append an existed key with different value.
    builder.appendIfAbsent("key1", "value2");
    String[] items = builder.build().getContext().split("\\$");
    Assert.assertEquals(1, items.length);
    Assert.assertEquals("key1:value1",
        builder.build().getContext());

    // Append an absent key.
    builder.appendIfAbsent("key2", "value2");
    String[] items2 = builder.build().getContext().split("\\$");
    Assert.assertEquals(2, items2.length);
    Assert.assertEquals("key1:value1$key2:value2",
        builder.build().getContext());

    // Append a key that is a substring of an existing key.
    builder.appendIfAbsent("key", "value");
    String[] items3 = builder.build().getContext().split("\\$");
    Assert.assertEquals(3, items3.length);
    Assert.assertEquals("key1:value1$key2:value2$key:value",
        builder.build().getContext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNewBuilder() {
    Configuration conf = new Configuration();
    // Set illegal separator.
    conf.set(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY, "\t");
    CallerContext.Builder builder = new CallerContext.Builder(null, conf);
    builder.build();
  }
}
