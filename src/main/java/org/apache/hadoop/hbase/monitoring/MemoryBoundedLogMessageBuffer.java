/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.monitoring;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A size-bounded repository of alerts, which are kept
 * in a linked list. Alerts can be added, and they will
 * automatically be removed one by one when the specified heap
 * usage is exhausted.
 */
public class MemoryBoundedLogMessageBuffer {
  private final long maxSizeBytes;
  private long usage = 0;
  private LinkedList<LogMessage> messages;
  
  public MemoryBoundedLogMessageBuffer(long maxSizeBytes) {
    Preconditions.checkArgument(
        maxSizeBytes > 0);
    this.maxSizeBytes = maxSizeBytes;
    this.messages = Lists.newLinkedList();
  }
  
  /**
   * Append the given message to this buffer, automatically evicting
   * older messages until the desired memory limit is achieved.
   */
  public synchronized void add(String messageText) {
    LogMessage message = new LogMessage(messageText, System.currentTimeMillis());
    
    usage += message.estimateHeapUsage();
    messages.add(message);
    while (usage > maxSizeBytes) {
      LogMessage removed = messages.remove();
      usage -= removed.estimateHeapUsage();
      assert usage >= 0;
    }
  }
  
  /**
   * Dump the contents of the buffer to the given stream.
   */
  public synchronized void dumpTo(PrintWriter out) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    for (LogMessage msg : messages) {
      out.write(df.format(new Date(msg.timestamp)));
      out.write(" ");
      out.println(new String(msg.message, Charsets.UTF_8));
    }
  }
  
  synchronized List<LogMessage> getMessages() {
    // defensive copy
    return Lists.newArrayList(messages);
  }
 
  /**
   * Estimate the number of bytes this buffer is currently
   * using.
   */
  synchronized long estimateHeapUsage() {
    return usage;
  }
  
  private static class LogMessage {
    /** the error text, encoded in bytes to save memory */
    public final byte[] message;
    public final long timestamp;
    
    /**
     * Completely non-scientific estimate of how much one of these
     * objects takes, along with the LinkedList overhead. This doesn't
     * need to be exact, since we don't expect a ton of these alerts.
     */
    private static final long BASE_USAGE=100;
    
    public LogMessage(String message, long timestamp) {
      this.message = message.getBytes(Charsets.UTF_8);
      this.timestamp = timestamp;
    }
    
    public long estimateHeapUsage() {
      return message.length + BASE_USAGE;
    }
  }

}
