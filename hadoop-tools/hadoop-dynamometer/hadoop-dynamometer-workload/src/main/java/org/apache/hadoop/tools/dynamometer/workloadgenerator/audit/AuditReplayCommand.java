/**
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
package org.apache.hadoop.tools.dynamometer.workloadgenerator.audit;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a single command to be replayed by the workload
 * generator. It implements the {@link Delayed} interface so that they can be
 * fetched in timestamp order from a {@link java.util.concurrent.DelayQueue}.
 * You can use the {@link #getPoisonPill(long)} method to retrieve "Poison Pill"
 * {@link AuditReplayCommand} which has {@link #isPoison()} as true,
 * representing to a consumer(s) of the {@link java.util.concurrent.DelayQueue}
 * that it should stop processing further items and instead terminate itself.
 */
class AuditReplayCommand implements Delayed {

  private static final Logger LOG = LoggerFactory
      .getLogger(AuditReplayCommand.class);
  private static final Pattern SIMPLE_UGI_PATTERN = Pattern
      .compile("([^/@ ]*).*?");

  private long absoluteTimestamp;
  private String ugi;
  private String command;
  private String src;
  private String dest;
  private String sourceIP;

  AuditReplayCommand(long absoluteTimestamp, String ugi, String command,
      String src, String dest, String sourceIP) {
    this.absoluteTimestamp = absoluteTimestamp;
    this.ugi = ugi;
    this.command = command;
    this.src = src;
    this.dest = dest;
    this.sourceIP = sourceIP;
  }

  long getAbsoluteTimestamp() {
    return absoluteTimestamp;
  }

  String getSimpleUgi() {
    Matcher m = SIMPLE_UGI_PATTERN.matcher(ugi);
    if (m.matches()) {
      return m.group(1);
    } else {
      LOG.error("Error parsing simple UGI <{}>; falling back to current user",
          ugi);
      try {
        return UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        return "";
      }
    }
  }

  String getCommand() {
    return command;
  }

  String getSrc() {
    return src;
  }

  String getDest() {
    return dest;
  }

  String getSourceIP() {
    return sourceIP;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(absoluteTimestamp - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    return Long.compare(absoluteTimestamp,
        ((AuditReplayCommand) o).absoluteTimestamp);
  }

  /**
   * If true, the thread which consumes this item should not process any further
   * items and instead simply terminate itself.
   */
  boolean isPoison() {
    return false;
  }

  /**
   * A command representing a Poison Pill, indicating that the processing thread
   * should not process any further items and instead should terminate itself.
   * Always returns true for {@link #isPoison()}. It does not contain any other
   * information besides a timestamp; other getter methods wil return null.
   */
  private static final class PoisonPillCommand extends AuditReplayCommand {

    private PoisonPillCommand(long absoluteTimestamp) {
      super(absoluteTimestamp, null, null, null, null, null);
    }

    @Override
    boolean isPoison() {
      return true;
    }

  }

  static AuditReplayCommand getPoisonPill(long relativeTimestamp) {
    return new PoisonPillCommand(relativeTimestamp);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AuditReplayCommand)) {
      return false;
    }
    AuditReplayCommand o = (AuditReplayCommand) other;
    return absoluteTimestamp == o.absoluteTimestamp && ugi.equals(o.ugi)
        && command.equals(o.command) && src.equals(o.src) && dest.equals(o.dest)
        && sourceIP.equals(o.sourceIP);
  }

  @Override
  public int hashCode() {
    return Objects.hash(absoluteTimestamp, ugi, command, src, dest, sourceIP);
  }

  @Override
  public String toString() {
    return String.format("AuditReplayCommand(absoluteTimestamp=%d, ugi=%s, "
            + "command=%s, src=%s, dest=%s, sourceIP=%s",
        absoluteTimestamp, ugi, command, src, dest, sourceIP);
  }
}
