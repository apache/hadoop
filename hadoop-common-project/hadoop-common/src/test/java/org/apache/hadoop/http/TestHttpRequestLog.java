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
package org.apache.hadoop.http;

import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestHttpRequestLog {

  @Test
  public void testAppenderDefined() {
    RequestLog requestLog = HttpRequestLog.getRequestLog("test");
    assertNotNull(requestLog, "RequestLog should not be null");
    assertInstanceOf(CustomRequestLog.class, requestLog);
    CustomRequestLog crl = (CustomRequestLog) requestLog;
    assertInstanceOf(Slf4jRequestLogWriter.class, crl.getWriter());
    assertEquals(CustomRequestLog.EXTENDED_NCSA_FORMAT, crl.getFormatString());
  }
}
