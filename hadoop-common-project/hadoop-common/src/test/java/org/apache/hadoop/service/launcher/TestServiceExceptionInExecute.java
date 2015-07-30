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

import static org.apache.hadoop.service.launcher.testservices.ExceptionInExecuteLaunchableService.*;
import org.junit.Test;

/**
 * This test verifies that exceptions in the 
 * {@link LaunchableService#execute()} method are relayed if an instance of
 * an exit exceptions, and forwarded if not.
 */
public class TestServiceExceptionInExecute extends AbstractServiceLauncherTestBase {

  @Test
  public void testEx() throws Throwable {
    assertLaunchOutcome(EXIT_EXCEPTION_THROWN,
        OTHER_EXCEPTION_TEXT,
        NAME);
  }

  @Test
  public void testSLE() throws Throwable {
    assertLaunchOutcome(EXIT_OTHER_FAILURE, 
        SLE_TEXT,
        NAME, 
        ARG_THROW_SLE);
  }

  @Test
  public void testIOE() throws Throwable {
    assertLaunchOutcome(IOE_EXIT_CODE, 
        EXIT_IN_IOE_TEXT,
        NAME, 
        ARG_THROW_IOE);
  }

  @Test
  public void testThrowable() throws Throwable {
    assertLaunchOutcome(EXIT_EXCEPTION_THROWN, 
        "java.lang.OutOfMemoryError",
        NAME, 
        ARG_THROWABLE);
  }

}
