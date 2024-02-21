package org.apache.hadoop.yarn.event.multidispatcher;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.yarn.event.Event;

class MockEvent implements Event<MockEventType> {
  private static final Random RANDOM = new Random();

  private final MockEventType type = Math.random() < 0.5
      ? MockEventType.TYPE_1
      : MockEventType.TYPE_2;

  private final long timestamp = System.currentTimeMillis();

  private final String lockKey = Arrays.asList(
      "APP1",
      "APP2",
      null
  ).get(RANDOM.nextInt(3));

  @Override
  public MockEventType getType() {
    return type;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getLockKey() {
    return lockKey;
  }
}
