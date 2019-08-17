/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web;

import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.apache.hadoop.ozone.web.utils.OzoneUtils.getRequestID;

/**
 * Test Ozone Error Codes.
 */
public class TestErrorCode {
  /**
   * Test Error Generator functions.
   */
  @Test
  public void testErrorGen() {
    OzoneException e = ErrorTable
        .newError(ErrorTable.ACCESS_DENIED, getRequestID(), "/test/path",
                  "localhost");
    assertEquals("localhost", e.getHostID());
    assertEquals(ErrorTable.ACCESS_DENIED.getShortMessage(),
        e.getShortMessage());
  }

  @Test
  public void testErrorGenWithException() {
    OzoneException e =
        new OzoneException(ErrorTable.ACCESS_DENIED.getHttpCode(),
                           "short message", new Exception("Hello"));
    assertEquals("short message", e.getShortMessage());
    assertEquals("Hello", e.getMessage());
  }
}
