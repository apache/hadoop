/*
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

package org.apache.hadoop.service.launcher;

import org.junit.Test;

/**
 * As the exception is doing some formatting tricks, these
 * tests verify that exception arguments are being correctly
 * used as initializers
 */
public class TestServiceLaunchExceptionFormatting extends AbstractServiceLauncherTestBase {

  @Test
  public void testBasicExceptionFormatting() throws Throwable {

    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x", 32);
    assertTrue(ex.getMessage().contains("020"));
  }
  @Test
  public void testNotEnoughArgsExceptionFormatting() throws Throwable {

    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x");
    assertTrue(ex.getMessage().contains("%03x"));
  }

  @Test
  public void testInnerCause() throws Throwable {

    Exception cause = new Exception("cause");
    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x: %s", 32, cause);
    assertTrue(ex.getMessage().contains("020"));
    assertTrue(ex.getMessage().contains("cause"));
    assertSame(cause, ex.getCause());
  }

  @Test
  public void testInnerCauseNotInFormat() throws Throwable {

    Exception cause = new Exception("cause");
    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x:", 32, cause);
    assertTrue(ex.getMessage().contains("020"));
    assertFalse(ex.getMessage().contains("cause"));
    assertSame(cause, ex.getCause());
  }

}
