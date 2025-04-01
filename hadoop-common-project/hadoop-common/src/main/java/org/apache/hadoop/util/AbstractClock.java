package org.apache.hadoop.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static java.time.ZoneOffset.UTC;

/**
 * An abstract base class for Clocks with the following default behavior:
 * <ul>
 * <li>Zone-agnostic: always returns UTC, ignoring any other zones</li>
 * <li>millis-centric: shifts responsibility of subclasses to defining {@link #millis()},
 * creating an Instant based on it (instead of vice versa as in {@link java.time.Clock})</li>
 * </ul>
 * Subclasses that want to change this behavior can either override relevant methods or
 * subclass {@link java.time.Clock} directly.
 */
public abstract class AbstractClock extends Clock {

  @Override
  public ZoneId getZone() {
    return UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return this;
  }

  @Override
  public abstract long millis();

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }
}
