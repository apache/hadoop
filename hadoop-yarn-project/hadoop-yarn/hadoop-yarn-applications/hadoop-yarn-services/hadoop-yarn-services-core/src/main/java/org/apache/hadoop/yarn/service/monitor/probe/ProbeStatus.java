/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.monitor.probe;

import java.io.Serializable;
import java.util.Date;

/**
 * Status message of a probe. This is designed to be sent over the wire, though the exception
 * Had better be unserializable at the far end if that is to work.
 */
public final class ProbeStatus implements Serializable {
  private static final long serialVersionUID = 165468L;

  private long timestamp;
  private String timestampText;
  private boolean success;
  private boolean realOutcome;
  private String message;
  private Throwable thrown;
  private transient Probe originator;

  public ProbeStatus() {
  }

  public ProbeStatus(long timestamp, String message, Throwable thrown) {
    this.success = false;
    this.message = message;
    this.thrown = thrown;
    setTimestamp(timestamp);
  }

  public ProbeStatus(long timestamp, String message) {
    this.success = true;
    setTimestamp(timestamp);
    this.message = message;
    this.thrown = null;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    timestampText = new Date(timestamp).toString();
  }

  public boolean isSuccess() {
    return success;
  }

  /**
   * Set both the success and the real outcome bits to the same value
   * @param success the new value
   */
  public void setSuccess(boolean success) {
    this.success = success;
    realOutcome = success;
  }

  public String getTimestampText() {
    return timestampText;
  }

  public boolean getRealOutcome() {
    return realOutcome;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Throwable getThrown() {
    return thrown;
  }

  public void setThrown(Throwable thrown) {
    this.thrown = thrown;
  }

  /**
   * Get the probe that generated this result. May be null
   * @return a possibly null reference to a probe
   */
  public Probe getOriginator() {
    return originator;
  }

  /**
   * The probe has succeeded -capture the current timestamp, set
   * success to true, and record any other data needed.
   * @param probe probe
   */
  public void succeed(Probe probe) {
    finish(probe, true, probe.getName(), null);
  }

  /**
   * A probe has failed either because the test returned false, or an exception
   * was thrown. The {@link #success} field is set to false, any exception 
   * thrown is recorded.
   * @param probe probe that failed
   * @param thrown an exception that was thrown.
   */
  public void fail(Probe probe, Throwable thrown) {
    finish(probe, false, "Failure in " + probe, thrown);
  }

  public void finish(Probe probe, boolean succeeded, String text, Throwable thrown) {
    setTimestamp(System.currentTimeMillis());
    setSuccess(succeeded);
    originator = probe;
    message = text;
    this.thrown = thrown;
  }

  @Override
  public String toString() {
    LogEntryBuilder builder = new LogEntryBuilder("Probe Status");
    builder.elt("time", timestampText)
           .elt("outcome", (success ? "success" : "failure"));

    if (success != realOutcome) {
      builder.elt("originaloutcome", (realOutcome ? "success" : "failure"));
    }
    builder.elt("message", message);
    if (thrown != null) {
      builder.elt("exception", thrown);
    }

    return builder.toString();
  }

  /**
   * Flip the success bit on while the real outcome bit is kept false
   */
  public void markAsSuccessful() {
    success = true;
  }
}
