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

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Log4jWarningErrorMetricsAppender extends AppenderSkeleton {

  public static final String LOG_METRICS_APPENDER = "RM_LOG_METRICS_APPENDER";
  static final int MAX_MESSAGE_SIZE = 2048;

  static public class Element {
    public Long count;
    public Long timestampSeconds;

    Element(Long count, Long timestampSeconds) {
      this.count = count;
      this.timestampSeconds = timestampSeconds;
    }
  }

  static class PurgeElement implements Comparable<PurgeElement> {
    String message;
    Long timestamp;

    PurgeElement(String message, Long timestamp) {
      this.message = message;
      this.timestamp = timestamp;
    }

    public int compareTo(PurgeElement e) {
      if (e == null) {
        throw new NullPointerException("Null element passed to compareTo");
      }
      int ret = this.timestamp.compareTo(e.timestamp);
      if (ret != 0) {
        return ret;
      }
      return this.message.compareTo(e.message);
    }

    @Override
    public boolean equals(Object e) {
      if (e == null || !(e instanceof PurgeElement)) {
        return false;
      }
      if (e == this) {
        return true;
      }
      PurgeElement el = (PurgeElement) e;
      return (this.message.equals(el.message))
          && (this.timestamp.equals(el.timestamp));
    }

    @Override
    public int hashCode() {
      return this.timestamp.hashCode();
    }
  }

  Map<String, SortedMap<Long, Integer>> errors;
  Map<String, SortedMap<Long, Integer>> warnings;
  SortedMap<Long, Integer> errorsTimestampCount;
  SortedMap<Long, Integer> warningsTimestampCount;
  SortedSet<PurgeElement> errorsPurgeInformation;
  SortedSet<PurgeElement> warningsPurgeInformation;

  Timer cleanupTimer;
  long cleanupInterval;
  long messageAgeLimitSeconds;
  int maxUniqueMessages;

  final Object lock = new Object();

  /**
   * Create an appender to keep track of the errors and warnings logged by the
   * system. It will keep purge messages older than 2 days. It will store upto
   * the last 500 unique errors and the last 500 unique warnings. The thread to
   * purge message will run every 5 minutes, unless the 500 message limit is hit
   * earlier.
   */
  public Log4jWarningErrorMetricsAppender() {
    this(5 * 60, 24 * 60 * 60, 250);
  }

  /**
   * Create an appender to keep track of the errors and warnings logged by the
   * system.
   * 
   * @param cleanupIntervalSeconds
   *          the interval at which old messages are purged to prevent the
   *          message stores from growing unbounded
   * @param messageAgeLimitSeconds
   *          the maximum age of a message in seconds before it is purged from
   *          the store
   * @param maxUniqueMessages
   *          the maximum number of unique messages of each type we keep before
   *          we start purging
   */
  public Log4jWarningErrorMetricsAppender(int cleanupIntervalSeconds,
      long messageAgeLimitSeconds, int maxUniqueMessages) {
    super();
    errors = new HashMap<>();
    warnings = new HashMap<>();
    errorsTimestampCount = new TreeMap<>();
    warningsTimestampCount = new TreeMap<>();
    errorsPurgeInformation = new TreeSet<>();
    warningsPurgeInformation = new TreeSet<>();

    cleanupTimer = new Timer();
    cleanupInterval = cleanupIntervalSeconds * 1000;
    cleanupTimer.schedule(new ErrorAndWarningsCleanup(), cleanupInterval);
    this.messageAgeLimitSeconds = messageAgeLimitSeconds;
    this.maxUniqueMessages = maxUniqueMessages;
    this.setName(LOG_METRICS_APPENDER);
    this.setThreshold(Level.WARN);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void append(LoggingEvent event) {
    String message = event.getRenderedMessage();
    String[] throwableStr = event.getThrowableStrRep();
    if (throwableStr != null) {
      message = message + "\n" + StringUtils.join("\n", throwableStr);
      message =
          org.apache.commons.lang.StringUtils.left(message, MAX_MESSAGE_SIZE);
    }
    int level = event.getLevel().toInt();

    if (level == Level.WARN_INT || level == Level.ERROR_INT) {
      // store second level information
      Long eventTimeSeconds = event.getTimeStamp() / 1000;
      Map<String, SortedMap<Long, Integer>> map;
      SortedMap<Long, Integer> timestampsCount;
      SortedSet<PurgeElement> purgeInformation;
      if (level == Level.WARN_INT) {
        map = warnings;
        timestampsCount = warningsTimestampCount;
        purgeInformation = warningsPurgeInformation;
      } else {
        map = errors;
        timestampsCount = errorsTimestampCount;
        purgeInformation = errorsPurgeInformation;
      }
      updateMessageDetails(message, eventTimeSeconds, map, timestampsCount,
        purgeInformation);
    }
  }

  private void updateMessageDetails(String message, Long eventTimeSeconds,
      Map<String, SortedMap<Long, Integer>> map,
      SortedMap<Long, Integer> timestampsCount,
      SortedSet<PurgeElement> purgeInformation) {
    synchronized (lock) {
      if (map.containsKey(message)) {
        SortedMap<Long, Integer> tmp = map.get(message);
        Long lastMessageTime = tmp.lastKey();
        int value = 1;
        if (tmp.containsKey(eventTimeSeconds)) {
          value = tmp.get(eventTimeSeconds) + 1;
        }
        tmp.put(eventTimeSeconds, value);
        purgeInformation.remove(new PurgeElement(message, lastMessageTime));
      } else {
        SortedMap<Long, Integer> value = new TreeMap<>();
        value.put(eventTimeSeconds, 1);
        map.put(message, value);
        if (map.size() > maxUniqueMessages * 2) {
          cleanupTimer.cancel();
          cleanupTimer = new Timer();
          cleanupTimer.schedule(new ErrorAndWarningsCleanup(), 0);
        }
      }
      purgeInformation.add(new PurgeElement(message, eventTimeSeconds));
      int newValue = 1;
      if (timestampsCount.containsKey(eventTimeSeconds)) {
        newValue = timestampsCount.get(eventTimeSeconds) + 1;
      }
      timestampsCount.put(eventTimeSeconds, newValue);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    cleanupTimer.cancel();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean requiresLayout() {
    return false;
  }

  /**
   * Get the counts of errors in the time periods provided. Note that the counts
   * provided by this function may differ from the ones provided by
   * getErrorMessagesAndCounts since the message store is purged at regular
   * intervals to prevent it from growing without bounds, while the store for
   * the counts is purged less frequently.
   * 
   * @param cutoffs
   *          list of timestamp cutoffs(in seconds) for which the counts are
   *          desired
   * @return list of error counts in the time periods corresponding to cutoffs
   */
  public List<Integer> getErrorCounts(List<Long> cutoffs) {
    return this.getCounts(errorsTimestampCount, cutoffs);
  }

  /**
   * Get the counts of warnings in the time periods provided. Note that the
   * counts provided by this function may differ from the ones provided by
   * getWarningMessagesAndCounts since the message store is purged at regular
   * intervals to prevent it from growing without bounds, while the store for
   * the counts is purged less frequently.
   * 
   * @param cutoffs
   *          list of timestamp cutoffs(in seconds) for which the counts are
   *          desired
   * @return list of warning counts in the time periods corresponding to cutoffs
   */
  public List<Integer> getWarningCounts(List<Long> cutoffs) {
    return this.getCounts(warningsTimestampCount, cutoffs);
  }

  private List<Integer> getCounts(SortedMap<Long, Integer> map,
      List<Long> cutoffs) {
    List<Integer> ret = new ArrayList<>();
    Long largestCutoff = Collections.min(cutoffs);
    for (int i = 0; i < cutoffs.size(); ++i) {
      ret.add(0);
    }
    synchronized (lock) {
      Map<Long, Integer> submap = map.tailMap(largestCutoff);
      for (Map.Entry<Long, Integer> entry : submap.entrySet()) {
        for (int i = 0; i < cutoffs.size(); ++i) {
          if (entry.getKey() >= cutoffs.get(i)) {
            int tmp = ret.get(i);
            ret.set(i, tmp + entry.getValue());
          }
        }
      }
    }
    return ret;
  }

  /**
   * Get the errors and the number of occurrences for each of the errors for the
   * time cutoffs provided. Note that the counts provided by this function may
   * differ from the ones provided by getErrorCounts since the message store is
   * purged at regular intervals to prevent it from growing without bounds,
   * while the store for the counts is purged less frequently.
   * 
   * @param cutoffs
   *          list of timestamp cutoffs(in seconds) for which the counts are
   *          desired
   * @return list of maps corresponding for each cutoff provided; each map
   *         contains the error and the number of times the error occurred in
   *         the time period
   */
  public List<Map<String, Element>>
      getErrorMessagesAndCounts(List<Long> cutoffs) {
    return this.getElementsAndCounts(errors, cutoffs, errorsPurgeInformation);
  }

  /**
   * Get the warning and the number of occurrences for each of the warnings for
   * the time cutoffs provided. Note that the counts provided by this function
   * may differ from the ones provided by getWarningCounts since the message
   * store is purged at regular intervals to prevent it from growing without
   * bounds, while the store for the counts is purged less frequently.
   * 
   * @param cutoffs
   *          list of timestamp cutoffs(in seconds) for which the counts are
   *          desired
   * @return list of maps corresponding for each cutoff provided; each map
   *         contains the warning and the number of times the error occurred in
   *         the time period
   */
  public List<Map<String, Element>> getWarningMessagesAndCounts(
      List<Long> cutoffs) {
    return this.getElementsAndCounts(warnings, cutoffs, warningsPurgeInformation);
  }

  private List<Map<String, Element>> getElementsAndCounts(
      Map<String, SortedMap<Long, Integer>> map, List<Long> cutoffs,
      SortedSet<PurgeElement> purgeInformation) {
    if (purgeInformation.size() > maxUniqueMessages) {
      ErrorAndWarningsCleanup cleanup = new ErrorAndWarningsCleanup();
      long cutoff = Time.now() - (messageAgeLimitSeconds * 1000);
      cutoff = (cutoff / 1000);
      cleanup.cleanupMessages(map, purgeInformation, cutoff, maxUniqueMessages);
    }
    List<Map<String, Element>> ret = new ArrayList<>(cutoffs.size());
    for (int i = 0; i < cutoffs.size(); ++i) {
      ret.add(new HashMap<String, Element>());
    }
    synchronized (lock) {
      for (Map.Entry<String, SortedMap<Long, Integer>> element : map.entrySet()) {
        for (int i = 0; i < cutoffs.size(); ++i) {
          Map<String, Element> retMap = ret.get(i);
          SortedMap<Long, Integer> qualifyingTimes =
              element.getValue().tailMap(cutoffs.get(i));
          long count = 0;
          for (Map.Entry<Long, Integer> entry : qualifyingTimes.entrySet()) {
            count += entry.getValue();
          }
          if (!qualifyingTimes.isEmpty()) {
            retMap.put(element.getKey(),
              new Element(count, qualifyingTimes.lastKey()));
          }
        }
      }
    }
    return ret;
  }

  // getters and setters for log4j
  public long getCleanupInterval() {
    return cleanupInterval;
  }

  public void setCleanupInterval(long cleanupInterval) {
    this.cleanupInterval = cleanupInterval;
  }

  public long getMessageAgeLimitSeconds() {
    return messageAgeLimitSeconds;
  }

  public void setMessageAgeLimitSeconds(long messageAgeLimitSeconds) {
    this.messageAgeLimitSeconds = messageAgeLimitSeconds;
  }

  public int getMaxUniqueMessages() {
    return maxUniqueMessages;
  }

  public void setMaxUniqueMessages(int maxUniqueMessages) {
    this.maxUniqueMessages = maxUniqueMessages;
  }

  class ErrorAndWarningsCleanup extends TimerTask {

    @Override
    public void run() {
      long cutoff = Time.now() - (messageAgeLimitSeconds * 1000);
      cutoff = (cutoff / 1000);
      cleanupMessages(errors, errorsPurgeInformation, cutoff, maxUniqueMessages);
      cleanupMessages(warnings, warningsPurgeInformation, cutoff,
        maxUniqueMessages);
      cleanupCounts(errorsTimestampCount, cutoff);
      cleanupCounts(warningsTimestampCount, cutoff);
      try {
        cleanupTimer.schedule(new ErrorAndWarningsCleanup(), cleanupInterval);
      } catch (IllegalStateException ie) {
        // don't do anything since new timer is already scheduled
      }
    }

    void cleanupMessages(Map<String, SortedMap<Long, Integer>> map,
            SortedSet<PurgeElement> purgeInformation, long cutoff,
            int mapTargetSize) {

      PurgeElement el = new PurgeElement("", cutoff);
      synchronized (lock) {
        SortedSet<PurgeElement> removeSet = purgeInformation.headSet(el);
        Iterator<PurgeElement> it = removeSet.iterator();
        while (it.hasNext()) {
          PurgeElement p = it.next();
          map.remove(p.message);
          it.remove();
        }

        // don't keep more mapTargetSize keys
        if (purgeInformation.size() > mapTargetSize) {
          Object[] array = purgeInformation.toArray();
          int cutoffIndex = purgeInformation.size() - mapTargetSize;
          for (int i = 0; i < cutoffIndex; ++i) {
            PurgeElement p = (PurgeElement) array[i];
            map.remove(p.message);
            purgeInformation.remove(p);
          }
        }
      }
    }

    void cleanupCounts(SortedMap<Long, Integer> map, long cutoff) {
      synchronized (lock) {
        Iterator<Map.Entry<Long, Integer>> it = map.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Long, Integer> element = it.next();
          if (element.getKey() < cutoff) {
            it.remove();
          }
        }
      }
    }
  }

  // helper function
  public static Log4jWarningErrorMetricsAppender findAppender() {
    Enumeration appenders = Logger.getRootLogger().getAllAppenders();
    while(appenders.hasMoreElements()) {
      Object obj = appenders.nextElement();
      if(obj instanceof Log4jWarningErrorMetricsAppender) {
        return (Log4jWarningErrorMetricsAppender) obj;
      }
    }
    return null;
  }
}
