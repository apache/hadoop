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

import org.apache.hadoop.service.BreakableService;
import org.apache.hadoop.service.launcher.testservices.FailureTestService;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test service launcher interrupt handling.
 */
public class TestServiceInterruptHandling
    extends AbstractServiceLauncherTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestServiceInterruptHandling.class);

  @Test
  public void testRegisterAndRaise() throws Throwable {
    InterruptCatcher catcher = new InterruptCatcher();
    String name = IrqHandler.CONTROL_C;
    IrqHandler irqHandler = new IrqHandler(name, catcher);
    irqHandler.bind();
    assertEquals(0, irqHandler.getSignalCount());
    irqHandler.raise();
    // allow for an async event
    Thread.sleep(500);
    IrqHandler.InterruptData data = catcher.interruptData;
    assertNotNull("interrupt data", data);
    assertEquals(name, data.getName());
    assertEquals(1, irqHandler.getSignalCount());
  }

  @Test
  public void testInterruptEscalationShutdown() throws Throwable {
    ExitTrackingServiceLauncher<BreakableService> launcher =
        new ExitTrackingServiceLauncher<>(BreakableService.class.getName());
    BreakableService service = new BreakableService();
    launcher.setService(service);

    InterruptEscalator escalator =
        new InterruptEscalator(launcher, 500);

    // call the interrupt operation directly
    try {
      escalator.interrupted(new IrqHandler.InterruptData("INT", 3));
      fail("Expected an exception to be raised in " + escalator);
    } catch (ExitUtil.ExitException e) {
      assertExceptionDetails(EXIT_INTERRUPTED, "", e);
    }
    //the service is now stopped
    assertStopped(service);
    assertTrue("isSignalAlreadyReceived() == false in " + escalator,
        escalator.isSignalAlreadyReceived());
    assertFalse("isForcedShutdownTimedOut() == true in " + escalator,
        escalator.isForcedShutdownTimedOut());

    // now interrupt it a second time and expect it to escalate to a halt
    try {
      escalator.interrupted(new IrqHandler.InterruptData("INT", 3));
      fail("Expected an exception to be raised in " + escalator);
    } catch (ExitUtil.HaltException e) {
      assertExceptionDetails(EXIT_INTERRUPTED, "", e);
    }
  }

  @Test
  public void testBlockingShutdownTimeouts() throws Throwable {
    ExitTrackingServiceLauncher<FailureTestService> launcher =
        new ExitTrackingServiceLauncher<>(FailureTestService.class.getName());
    FailureTestService service =
        new FailureTestService(false, false, false, 2000);
    launcher.setService(service);

    InterruptEscalator escalator = new InterruptEscalator(launcher, 500);
    // call the interrupt operation directly
    try {
      escalator.interrupted(new IrqHandler.InterruptData("INT", 3));
      fail("Expected an exception to be raised from " + escalator);
    } catch (ExitUtil.ExitException e) {
      assertExceptionDetails(EXIT_INTERRUPTED, "", e);
    }

    assertTrue("isForcedShutdownTimedOut() == false in " + escalator,
        escalator.isForcedShutdownTimedOut());
  }

  private static class InterruptCatcher implements IrqHandler.Interrupted {

    public IrqHandler.InterruptData interruptData;

    @Override
    public void interrupted(IrqHandler.InterruptData data) {
      LOG.info("Interrupt caught");
      this.interruptData = data;
    }
  }

}
