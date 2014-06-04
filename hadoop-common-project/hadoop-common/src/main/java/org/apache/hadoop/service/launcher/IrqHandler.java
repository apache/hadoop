/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
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

package org.apache.hadoop.service.launcher;

import com.google.common.base.Preconditions;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Handler of interrupts -relays them to a registered
 * implementation of {@link IrqHandler.Interrupted}
 *
 * This class bundles up all the compiler warnings about abuse of sun.misc
 * interrupt handling code into one place.
 */
@SuppressWarnings("UseOfSunClasses")
public final class IrqHandler implements SignalHandler {

  /**
   * Definition of the Control-C handler name: {@value}
   */
  public static final String CONTROL_C = "INT";

  /**
   * Definition of default <code>kill</code> signal: {@value}
   */
  public static final String SIGTERM = "TERM";

  private final String name;

  /**
   * Handler to relay to
   */
  private final Interrupted handler;

  /**
   * Create an IRQ handler bound to the specific interrupt
   * @param name signal name
   * @param handler handler
   * @throws IllegalArgumentException if the exception could not be set
   */
  public IrqHandler(String name, Interrupted handler) {
    Preconditions.checkArgument(name != null, "Null \"name\"");
    Preconditions.checkArgument(handler != null, "Null \"handler\"");
    this.handler = handler;
    this.name = name;
    try {
      Signal.handle(new Signal(name), this);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Could not set handler for signal \"" + name + "\"."
          + "This can happen if the JVM has the -Xrs set.",
          e);
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "IrqHandler for signal " + name;
  }

  /**
   * Handler for the JVM API for signal handling
   * @param signal signal raised
   */
  @Override
  public void handle(Signal signal) {
    InterruptData data =
        new InterruptData(signal.getName(), signal.getNumber());
    handler.interrupted(data);
  }

  /**
   * Callback issues on an interrupt
   */
  public interface Interrupted {

    /**
     * Handle an interrupt
     * @param interruptData data
     */
    void interrupted(InterruptData interruptData);
  }

  /**
   * Interrupt data to pass on.
   */
  public static class InterruptData {
    public final String name;
    public final int number;

    public InterruptData(String name, int number) {
      this.name = name;
      this.number = number;
    }

    @Override
    public String toString() {
      return "signal " + name + '(' + number + ')';
    }
  }
}