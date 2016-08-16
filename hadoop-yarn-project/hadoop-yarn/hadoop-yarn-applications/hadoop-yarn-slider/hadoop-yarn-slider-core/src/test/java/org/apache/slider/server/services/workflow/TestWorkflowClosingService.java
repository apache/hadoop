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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;

public class TestWorkflowClosingService extends WorkflowServiceTestBase {

  @Test
  public void testSimpleClose() throws Throwable {
    ClosingService<OpenClose> svc = instance(false);
    OpenClose openClose = svc.getCloseable();
    assertFalse(openClose.closed);
    svc.stop();
    assertTrue(openClose.closed);
  }

  @Test
  public void testNullClose() throws Throwable {
    ClosingService<OpenClose> svc = new ClosingService<OpenClose>("", null);
    svc.init(new Configuration());
    svc.start();
    assertNull(svc.getCloseable());
    svc.stop();
  }

  @Test
  public void testFailingClose() throws Throwable {
    ClosingService<OpenClose> svc = instance(false);
    OpenClose openClose = svc.getCloseable();
    openClose.raiseExceptionOnClose = true;
    svc.stop();
    assertTrue(openClose.closed);
    Throwable cause = svc.getFailureCause();
    assertNotNull(cause);

    //retry should be a no-op
    svc.close();
  }

  @Test
  public void testDoubleClose() throws Throwable {
    ClosingService<OpenClose> svc = instance(false);
    OpenClose openClose = svc.getCloseable();
    openClose.raiseExceptionOnClose = true;
    svc.stop();
    assertTrue(openClose.closed);
    Throwable cause = svc.getFailureCause();
    assertNotNull(cause);
    openClose.closed = false;
    svc.stop();
    assertEquals(cause, svc.getFailureCause());
  }

  /**
   * This does not recurse forever, as the service has already entered the
   * STOPPED state before the inner close tries to stop it -that operation
   * is a no-op
   * @throws Throwable
   */
  @Test
  public void testCloseSelf() throws Throwable {
    ClosingService<ClosingService> svc =
        new ClosingService<ClosingService>("");
    svc.setCloseable(svc);
    svc.stop();
  }


  private ClosingService<OpenClose> instance(boolean raiseExceptionOnClose) {
    ClosingService<OpenClose> svc = new ClosingService<OpenClose>(new OpenClose(
        raiseExceptionOnClose));
    svc.init(new Configuration());
    svc.start();
    return svc;
  }

  private static class OpenClose implements Closeable {
    public boolean closed = false;
    public boolean raiseExceptionOnClose;

    private OpenClose(boolean raiseExceptionOnClose) {
      this.raiseExceptionOnClose = raiseExceptionOnClose;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        if (raiseExceptionOnClose) {
          throw new IOException("OpenClose");
        }
      }
    }
  }
}
