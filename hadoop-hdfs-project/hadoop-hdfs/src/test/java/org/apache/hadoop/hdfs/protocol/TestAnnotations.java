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
package org.apache.hadoop.hdfs.protocol;

import java.lang.reflect.Method;

import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests to make sure all the protocol class public methods have
 * either {@link Idempotent} or {@link AtMostOnce} once annotations.
 */
public class TestAnnotations {
  @Test
  public void checkAnnotations() {
    Method[] methods = NamenodeProtocols.class.getMethods();
    for (Method m : methods) {
      Assert.assertTrue(
          "Idempotent or AtMostOnce annotation is not present " + m,
          m.isAnnotationPresent(Idempotent.class)
              || m.isAnnotationPresent(AtMostOnce.class));
    }
  }
}
