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

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.ObjectWriter;
import org.apache.htrace.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoggerSpanReceiver is a layer between HTrace and log4j only used for {@link org.apache.hadoop.fs.azurebfs.contracts.services.TracingService}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LoggerSpanReceiver extends SpanReceiver {
  private static final ObjectWriter JSON_WRITER =
      new ObjectMapper()
          .configure(SerializationFeature.INDENT_OUTPUT, true)
          .configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true)
          .configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false)
          .configure(SerializationFeature.USE_EQUALITY_FOR_OBJECT_ID, false)
          .writer();

  public LoggerSpanReceiver(HTraceConfiguration hTraceConfiguration) {
    Preconditions.checkNotNull(hTraceConfiguration, "hTraceConfiguration");
  }

  @Override
  public void receiveSpan(final Span span) {
    String jsonValue;

    Logger logger = LoggerFactory.getLogger(AzureBlobFileSystem.class);

    try {
      jsonValue = JSON_WRITER.writeValueAsString(span);
      logger.trace(jsonValue);
    } catch (JsonProcessingException e) {
      logger.error("Json processing error: " + e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    // No-Op
  }
}