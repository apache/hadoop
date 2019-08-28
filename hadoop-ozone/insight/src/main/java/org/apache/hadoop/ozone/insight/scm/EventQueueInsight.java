/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight.scm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;

/**
 * Insight definition to check internal events.
 */
public class EventQueueInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers
        .add(new LoggerSource(Type.SCM, EventQueue.class,
            defaultLevel(verbose)));
    return loggers;
  }

  @Override
  public String getDescription() {
    return "Information about the internal async event delivery";
  }

}
