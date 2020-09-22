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

import org.junit.Assert;
import org.junit.Test;

public class TestCallerContext {
  @Test
  public void testBuilderAppend() {
    CallerContext.Builder builder = new CallerContext.Builder("context1");
    CallerContext context =
        builder.append("context2").append("key3", "value3", ":").build();
    Assert.assertEquals(true,
        context.getContext().contains(CallerContext.ITEM_SEPARATOR));
    String[] items = context.getContext().split(CallerContext.ITEM_SEPARATOR);
    Assert.assertEquals(3, items.length);
    Assert.assertEquals(true, items[2].equals("key3:value3"));
  }
}
