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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TraceScope;

/**
 * Test tracing service.
 */
public class ITestTracingServiceImpl extends DependencyInjectedTest {
  private final List<String> messageStorage;

  public ITestTracingServiceImpl() {
    super();
    this.messageStorage = new ArrayList<>();
  }

  @Test
  public void traceSerializationTest() throws Exception {
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.TRACE);
    rootLogger.addAppender(new AppenderSkeleton() {
      @Override
      protected void append(LoggingEvent event) {
        if (event.getLogger().getName().indexOf("AzureBlobFileSystem") != -1) {
          messageStorage.add(event.getRenderedMessage());
        }
      }

      @Override
      public void close() {

      }

      @Override
      public boolean requiresLayout() {
        return false;
      }
    });

    TracingService tracingService = new TracingServiceImpl(new Configuration());
    TraceScope traceScope = tracingService.traceBegin("Test Scope");
    traceScope.addTimelineAnnotation("Timeline Annotations");
    traceScope.addKVAnnotation("key", "value");
    traceScope.close();

    // Should not throw exception.
    MilliSpan.fromJson(messageStorage.get(0));
  }
}
