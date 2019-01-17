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

package org.apache.hadoop.ozone.s3.exception;

import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * This class tests OS3Exception class.
 */
public class TestOS3Exception {

  @Test
  public void testOS3Exception() {
    OS3Exception ex = new OS3Exception("AccessDenied", "Access Denied",
        403);
    String requestId = OzoneUtils.getRequestID();
    ex = S3ErrorTable.newError(ex, "bucket");
    ex.setRequestId(requestId);
    String val = ex.toXml();
    String formatString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<Error>\n" +
        "  <Code>%s</Code>\n" +
        "  <Message>%s</Message>\n" +
        "  <Resource>%s</Resource>\n" +
        "  <RequestId>%s</RequestId>\n" +
        "</Error>\n";
    String expected = String.format(formatString, ex.getCode(),
        ex.getErrorMessage(), ex.getResource(),
        ex.getRequestId());
    Assert.assertEquals(expected, val);
  }
}
