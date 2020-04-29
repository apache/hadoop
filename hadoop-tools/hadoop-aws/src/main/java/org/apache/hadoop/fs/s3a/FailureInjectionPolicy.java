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

package org.apache.hadoop.fs.s3a;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Simple object which stores current failure injection settings.
 * "Delaying a key" can mean:
 *    - Removing it from the S3 client's listings while delay is in effect.
 *    - Causing input stream reads to fail.
 *    - Causing the S3 side of getFileStatus(), i.e.
 *      AmazonS3#getObjectMetadata(), to throw FileNotFound.
 */
public class FailureInjectionPolicy {
  /**
   * Keys containing this substring will be subject to delayed visibility.
   */
  public static final String DEFAULT_DELAY_KEY_SUBSTRING = "DELAY_LISTING_ME";

  /**
   * How many seconds affected keys will have delayed visibility.
   * This should probably be a config value.
   */
  public static final long DEFAULT_DELAY_KEY_MSEC = 5 * 1000;

  public static final float DEFAULT_DELAY_KEY_PROBABILITY = 1.0f;

  /** Special config value since we can't store empty strings in XML. */
  public static final String MATCH_ALL_KEYS = "*";

  private static final Logger LOG =
      LoggerFactory.getLogger(InconsistentAmazonS3Client.class);

  /** Empty string matches all keys. */
  private String delayKeySubstring;

  /** Probability to delay visibility of a matching key. */
  private float delayKeyProbability;

  /** Time in milliseconds to delay visibility of newly modified object. */
  private long delayKeyMsec;

  /**
   * Probability of throttling a request.
   */
  private float throttleProbability;

  /**
   * limit for failures before operations succeed; if 0 then "no limit".
   */
  private int failureLimit = 0;

  public FailureInjectionPolicy(Configuration conf) {

    this.delayKeySubstring = conf.get(FAIL_INJECT_INCONSISTENCY_KEY,
        DEFAULT_DELAY_KEY_SUBSTRING);
    // "" is a substring of all strings, use it to match all keys.
    if (this.delayKeySubstring.equals(MATCH_ALL_KEYS)) {
      this.delayKeySubstring = "";
    }
    this.delayKeyProbability = validProbability(
        conf.getFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY,
            DEFAULT_DELAY_KEY_PROBABILITY));
    this.delayKeyMsec = conf.getLong(FAIL_INJECT_INCONSISTENCY_MSEC,
        DEFAULT_DELAY_KEY_MSEC);
    this.setThrottleProbability(conf.getFloat(FAIL_INJECT_THROTTLE_PROBABILITY,
        0.0f));
  }

  public String getDelayKeySubstring() {
    return delayKeySubstring;
  }

  public float getDelayKeyProbability() {
    return delayKeyProbability;
  }

  public long getDelayKeyMsec() {
    return delayKeyMsec;
  }

  public float getThrottleProbability() {
    return throttleProbability;
  }

  public int getFailureLimit() {
    return failureLimit;
  }

  public void setFailureLimit(int failureLimit) {
    this.failureLimit = failureLimit;
  }

  /**
   * Set the probability of throttling a request.
   * @param throttleProbability the probability of a request being throttled.
   */
  public void setThrottleProbability(float throttleProbability) {
    this.throttleProbability = validProbability(throttleProbability);
  }

  public static boolean trueWithProbability(float p) {
    return Math.random() < p;
  }

  /**
   * Should we delay listing visibility for this key?
   * @param key key which is being put
   * @return true if we should delay
   */
  public boolean shouldDelay(String key) {
    float p = getDelayKeyProbability();
    boolean delay = key.contains(getDelayKeySubstring());
    delay = delay && trueWithProbability(p);
    LOG.debug("{}, p={} -> {}", key, p, delay);
    return delay;
  }

  @Override
  public String toString() {
    return String.format("FailureInjectionPolicy:" +
            " %s msec delay, substring %s, delay probability %s;" +
            " throttle probability %s" + "; failure limit %d",
        delayKeyMsec, delayKeySubstring, delayKeyProbability,
        throttleProbability, failureLimit);
  }

  /**
   * Validate a probability option.
   * @param p probability
   * @return the probability, if valid
   * @throws IllegalArgumentException if the probability is out of range.
   */
  private static float validProbability(float p) {
    Preconditions.checkArgument(p >= 0.0f && p <= 1.0f,
        "Probability out of range 0 to 1 %s", p);
    return p;
  }

}
