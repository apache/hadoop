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

package org.apache.slider.server.appmaster.actions;

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;

import java.io.IOException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AsyncAction implements Delayed {

  private static final AtomicLong sequencer = new AtomicLong(0);

  public final String name;
  private long nanos;
  public final int attrs;
  private final long sequenceNumber = sequencer.incrementAndGet();


  protected AsyncAction(String name) {
    this(name, 0);
  }

  protected AsyncAction(String name,
      long delayMillis) {
    this(name, delayMillis, TimeUnit.MILLISECONDS);
  }

  protected AsyncAction(String name,
      long delay,
      TimeUnit timeUnit) {
    this(name, delay, timeUnit, 0);
  }

  protected AsyncAction(String name,
      long delay,
      TimeUnit timeUnit,
      int attrs) {
    this.name = name;
    this.setNanos(convertAndOffset(delay, timeUnit));
    this.attrs = attrs;
  }

  protected long convertAndOffset(long delay, TimeUnit timeUnit) {
    return now() + TimeUnit.NANOSECONDS.convert(delay, timeUnit);
  }

  /**
   * The current time in nanos
   * @return now
   */
  protected long now() {
    return System.nanoTime();
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(getNanos() - now(), TimeUnit.NANOSECONDS);
  }

  @Override
  public int compareTo(Delayed that) {
    if (this == that) {
      return 0;
    }
    return SliderUtils.compareTo(
        getDelay(TimeUnit.NANOSECONDS),
        that.getDelay(TimeUnit.NANOSECONDS));
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(super.toString());
    sb.append(" name='").append(name).append('\'');
    sb.append(", delay=").append(getDelay(TimeUnit.SECONDS));
    sb.append(", attrs=").append(attrs);
    sb.append(", sequenceNumber=").append(sequenceNumber);
    sb.append('}');
    return sb.toString();
  }

  protected int getAttrs() {
    return attrs;
  }

  /**
   * Ask if an action has an of the specified bits set. 
   * This is not an equality test.
   * @param attr attribute
   * @return true iff the action has any of the bits in the attr arg set
   */
  public boolean hasAttr(int attr) {
    return (attrs & attr) != 0;
  }

  /**
   * Actual application
   * @param appMaster
   * @param queueService
   * @param appState
   * @throws IOException
   */
  public abstract void execute(SliderAppMaster appMaster,
      QueueAccess queueService, AppState appState) throws Exception;

  public long getNanos() {
    return nanos;
  }

  public void setNanos(long nanos) {
    this.nanos = nanos;
  }
  
  public static final int ATTR_CHANGES_APP_SIZE = 1;
  public static final int ATTR_HALTS_APP = 2;
  public static final int ATTR_REVIEWS_APP_SIZE = 4;
}
