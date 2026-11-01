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
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ExceptionHandler}, with focus on HDFS-17796: the NameNode
 * WebHDFS handler should map storage-capacity exceptions to HTTP 507 instead
 * of the generic 403 / 500 it returned before.
 */
public class TestExceptionHandler {

  private ExceptionHandler newHandler() {
    ExceptionHandler eh = new ExceptionHandler();
    eh.initResponse(mock(HttpServletResponse.class));
    return eh;
  }

  @Test
  public void testDSQuotaExceededMapsTo507() {
    Response resp = newHandler().toResponse(
        new DSQuotaExceededException("disk quota exceeded"));
    assertEquals(507, resp.getStatus());
  }

  @Test
  public void testNSQuotaExceededMapsTo507() {
    // NSQuotaExceededException also extends ClusterStorageCapacityExceededException
    // via QuotaExceededException, so it shares the new mapping.
    Response resp = newHandler().toResponse(
        new NSQuotaExceededException("namespace quota exceeded"));
    assertEquals(507, resp.getStatus());
  }

  @Test
  public void testDiskOutOfSpaceMapsTo507() {
    Response resp = newHandler().toResponse(
        new DiskOutOfSpaceException("no space left on device"));
    assertEquals(507, resp.getStatus());
  }

  @Test
  public void testGenericIOExceptionStillMapsTo403() {
    Response resp = newHandler().toResponse(
        new IOException("some unrelated IO failure"));
    assertEquals(403, resp.getStatus());
  }

  @Test
  public void testFileNotFoundStillMapsTo404() {
    Response resp = newHandler().toResponse(new FileNotFoundException("missing"));
    assertEquals(404, resp.getStatus());
  }

  @Test
  public void testIllegalArgumentStillMapsTo400() {
    Response resp = newHandler().toResponse(new IllegalArgumentException("bad"));
    assertEquals(400, resp.getStatus());
  }
}
