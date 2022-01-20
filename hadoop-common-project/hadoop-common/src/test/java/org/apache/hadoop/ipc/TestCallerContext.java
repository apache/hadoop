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

import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY;

public class TestCallerContext {
  @Test
  public void testBuilderAppend() {
    Configuration conf = new Configuration();
    conf.set(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY, "$");
    CallerContext.Builder builder = new CallerContext.Builder(null, conf, true);
    CallerContext context =
        builder.append("context1").append("context2").append("key3", "value3")
            .build();
    Assert.assertEquals(true, context.getContext().contains("$"));
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
    CallerContext.Builder builder = new CallerContext.Builder(null, conf, true);
    builder.append("key1", "value1");
    Assert.assertEquals("key1:value1", builder.build().getContext());

    // Append an existed key with different value.
    builder.appendIfAbsent("key1", "value2");
    String[] items = builder.build().getContext().split("\\$");
    Assert.assertEquals(1, items.length);
    Assert.assertEquals("key1:value1", builder.build().getContext());

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

  @Test()
  public void testNewBuilder() {
    Configuration conf = new Configuration();
    // Set illegal separator.
    conf.set(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY, "\t");
    CallerContext.Builder builder =
        new CallerContext.Builder("filed1", conf, true);
    builder.append("filed2");
    String context = builder.build().getContext();
    // assert use default separator ',' instead
    Assert.assertEquals(context, "filed1,filed2");
  }

  @Test()
  public void testContextLengthExceedLimit() {

    Configuration conf = new Configuration();
    conf.set(HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY, "16");
    conf.set(HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY, "8");
    CallerContext.Builder builder =
        new CallerContext.Builder("filed1", conf, true);
    builder.setSignature("sig1".getBytes(StandardCharsets.UTF_8));
    CallerContext context = builder.build();

    Assert.assertEquals(context.getContext().length(), 6);
    Assert.assertEquals(context.getSignature().length, 4);

    builder = new CallerContext.Builder("filed1", conf, true);
    builder.append("filed2").append("filed3").append("filed4");
    builder.setSignature("signature".getBytes(StandardCharsets.UTF_8));
    context = builder.build();

    // assert context was truncated
    Assert.assertEquals(context.getContext().length(), 16);
    // assert signature was abandoned
    Assert.assertNull(context.getSignature());
  }

  @Test()
  public void testWrongLengthLimitConfigs() {

    Configuration conf = new Configuration();

    // Set illegal size limit.
    conf.set(HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY, "-1");
    conf.set(HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY, "-1");

    CallerContext.Builder builder =
        new CallerContext.Builder("filed1", conf, true);
    builder.setSignature("signature1".getBytes(StandardCharsets.UTF_8));
    CallerContext context = builder.build();

    Assert.assertEquals(context.getContext().length(), 6);
    Assert.assertEquals(context.getSignature().length, 10);

    StringBuilder sig = new StringBuilder();
    builder = new CallerContext.Builder("", conf, true);
    for (int i = 0; i < 20; i++) {
      builder.append("filed").append(String.valueOf(i));
      sig.append("s").append(i);
    }

    builder.setSignature(sig.toString().getBytes(StandardCharsets.UTF_8));
    context = builder.build();

    // assert context was truncated to default length
    Assert.assertEquals(context.getContext().length(),
        HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT);
    // assert signature was abandoned
    Assert.assertNull(context.getSignature());
  }

}
