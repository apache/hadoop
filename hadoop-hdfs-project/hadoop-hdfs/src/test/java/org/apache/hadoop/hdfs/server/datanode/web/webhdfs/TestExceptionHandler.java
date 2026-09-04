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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ExceptionHandler}, with focus on HDFS-17796: writes that
 * fail because of insufficient storage should return HTTP 507 instead of the
 * generic 403 / 500 the handler used to produce.
 */
public class TestExceptionHandler {

  @Test
  public void testDSQuotaExceededMapsTo507() {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(
        new DSQuotaExceededException("disk quota exceeded"));
    assertEquals(507, resp.status().code());
  }

  @Test
  public void testNSQuotaExceededMapsTo507() {
    // NSQuotaExceededException also extends ClusterStorageCapacityExceededException
    // via QuotaExceededException, so it gets the new mapping for free.
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(
        new NSQuotaExceededException("namespace quota exceeded"));
    assertEquals(507, resp.status().code());
  }

  @Test
  public void testDiskOutOfSpaceMapsTo507() {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(
        new DiskOutOfSpaceException("no space left on device"));
    assertEquals(507, resp.status().code());
  }

  @Test
  public void testGenericIOExceptionStillMapsTo403() {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(
        new IOException("some unrelated IO failure"));
    assertEquals(403, resp.status().code());
  }

  @Test
  public void testFileNotFoundStillMapsTo404() {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(
        new FileNotFoundException("missing"));
    assertEquals(404, resp.status().code());
  }

  @Test
  public void testIllegalArgumentStillMapsTo400() {
    DefaultFullHttpResponse resp = ExceptionHandler.exceptionCaught(
        new IllegalArgumentException("bad param"));
    assertEquals(400, resp.status().code());
  }
}
