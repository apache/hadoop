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

import java.util.concurrent.atomic.AtomicInteger;

import jnr.constants.platform.Signal;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import jnr.posix.SignalHandler;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Handler of interrupts that relays them to a registered
 * implementation of {@link IrqHandler.Interrupted}.
 *
 * This class bundles up all the compiler warnings about abuse of sun.misc
 * interrupt handling code into one place.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings("UseOfSunClasses")
public final class IrqHandler implements SignalHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IrqHandler.class);
  
  /**
   * Definition of the Control-C handler name: {@value}.
   */
  public static final String CONTROL_C = "SIGINT";

  /**
   * Definition of default <code>kill</code> signal: {@value}.
   */
  public static final String SIGTERM = "SIGTERM";

  /**
   * Handler to relay to.
   */
  private final Interrupted handler;
  private static final POSIX POSIX_IMPL = POSIXFactory.getNativePOSIX();

  /** Count of how many times a signal has been raised. */
  private final AtomicInteger signalCount = new AtomicInteger(0);

  /**
   * Stored signal.
   */
  private Signal signal;

  /**
   * Create an IRQ handler bound to the specific interrupt.
   * @param signal signal
   * @param handler handler
   */
  public IrqHandler(Signal signal, Interrupted handler) {
    Preconditions.checkArgument(signal != null, "Null \"signal\"");
    Preconditions.checkArgument(handler != null, "Null \"handler\"");
    this.handler = handler;
    this.signal = signal;
  }

  /**
   * Bind to the interrupt handler.
   * @throws IllegalArgumentException if the exception could not be set
   */
  public void bind() {
    try {
      POSIX_IMPL.signal(signal, this);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Could not set handler for signal \"" + signal + "\"."
          + "This can happen if the JVM has the -Xrs set.",
          e);
    }
  }

  /**
   * @return the signal name.
   */
  public String getName() {
    return signal.name();
  }

  /**
   * Raise the signal.
   */
  public void raise() {
    POSIX_IMPL.kill(POSIX_IMPL.getpid(), signal.intValue());
  }

  @Override
  public String toString() {
    return "IrqHandler for signal " + signal.name();
  }

  /**
   * Handler for the JVM API for signal handling.
   * @param signalNumber signal raised
   */
  @Override
  public void handle(int signalNumber) {
    signalCount.incrementAndGet();
    InterruptData data = new InterruptData(signal.name(), signalNumber);
    LOG.info("Interrupted: {}", data);
    handler.interrupted(data);
  }

  /**
   * Get the count of how many times a signal has been raised.
   * @return the count of signals
   */
  public int getSignalCount() {
    return signalCount.get();
  }
  
  /**
   * Callback issues on an interrupt.
   */
  public interface Interrupted {

    /**
     * Handle an interrupt.
     * @param interruptData data
     */
    void interrupted(InterruptData interruptData);
  }

  /**
   * Interrupt data to pass on.
   */
  public static class InterruptData {
    private final String name;
    private final int number;

    public InterruptData(String name, int number) {
      this.name = name;
      this.number = number;
    }

    public String getName() {
      return name;
    }

    public int getNumber() {
      return number;
    }

    @Override
    public String toString() {
      return "signal " + name + '(' + number + ')';
    }
  }
}
