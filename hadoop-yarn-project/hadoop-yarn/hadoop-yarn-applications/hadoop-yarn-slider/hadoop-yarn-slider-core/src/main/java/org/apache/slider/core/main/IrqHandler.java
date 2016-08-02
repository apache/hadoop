/*
 *  Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.slider.core.main;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;

/**
 * This class bundles up all the compiler warnings about abuse of sun.misc
 * interrupt handling code
 * into one place.
 */
@SuppressWarnings("UseOfSunClasses")
public final class IrqHandler implements SignalHandler {

  public static final String CONTROL_C = "INT";
  public static final String SIGTERM = "TERM";

  private final String name;
  private final Interrupted handler;

  /**
   * Create an IRQ handler bound to the specific interrupt
   * @param name signal name
   * @param handler handler
   * @throws IOException
   */
  public IrqHandler(String name, Interrupted handler) throws IOException {
    this.handler = handler;
    this.name = name;
    try {
      Signal.handle(new Signal(name), this);
    } catch (IllegalArgumentException e) {
      throw new IOException(
        "Could not set handler for signal \"" + name + "\"."
        + "This can happen if the JVM has the -Xrs set.",
        e);
    }
  }

  @Override
  public String toString() {
    return "IrqHandler for signal " + name ;
  }

  /**
   * Handler for the JVM API for signal handling
   * @param signal signal raised
   */
//  @Override
  public void handle(Signal signal) {
    InterruptData data = new InterruptData(signal.getName(), signal.getNumber());
    handler.interrupted(data);
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

  /**
   * Callback on interruption
   */
  public interface Interrupted {

    /**
     * Handle an interrupt
     * @param interruptData data
     */
    void interrupted(InterruptData interruptData);
  }
}