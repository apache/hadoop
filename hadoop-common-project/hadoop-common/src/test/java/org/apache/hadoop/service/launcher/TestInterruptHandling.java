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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test service launcher interrupt handling
 */
public class TestInterruptHandling extends AbstractServiceLauncherTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestInterruptHandling.class);
  
  @Test
  public void testRegisterAndRaise() throws Throwable {
    InterruptCatcher catcher = new InterruptCatcher();
    String name = IrqHandler.CONTROL_C;
    IrqHandler irqHandler = new IrqHandler(name, catcher);
    assertEquals(0, irqHandler.getSignalCount());
    irqHandler.raise();
    // allow for an async event
    Thread.sleep(500);
    IrqHandler.InterruptData data = catcher.interruptData;
    assertNotNull(data);
    assertEquals(name, data.name);
    assertEquals(1, irqHandler.getSignalCount());
  }

  private static class InterruptCatcher implements IrqHandler.Interrupted {

    IrqHandler.InterruptData interruptData;

    @Override
    public void interrupted(IrqHandler.InterruptData interruptData) {
      LOG.info("Interrupt caught");
      this.interruptData = interruptData;
    }
  }
}
