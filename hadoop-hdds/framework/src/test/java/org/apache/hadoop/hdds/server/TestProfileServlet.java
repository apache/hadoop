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
package org.apache.hadoop.hdds.server;

import java.io.IOException;

import org.apache.hadoop.hdds.server.ProfileServlet.Event;
import org.apache.hadoop.hdds.server.ProfileServlet.Output;

import org.junit.Test;

/**
 * Test prometheus Sink.
 */
public class TestProfileServlet {

  @Test
  public void testNameValidation() throws IOException {
    ProfileServlet.validateFileName(
        ProfileServlet.generateFileName(1, Output.SVG, Event.ALLOC));

    ProfileServlet.validateFileName(
        ProfileServlet.generateFileName(23, Output.COLLAPSED,
            Event.L1_DCACHE_LOAD_MISSES));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameValidationWithNewLine() throws IOException {
    ProfileServlet.validateFileName(
        "test\n" + ProfileServlet.generateFileName(1, Output.SVG, Event.ALLOC));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNameValidationWithSlash() throws IOException {
    ProfileServlet.validateFileName(
        "../" + ProfileServlet.generateFileName(1, Output.SVG, Event.ALLOC));
  }

}