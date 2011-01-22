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
package org.apache.hadoop.tools.rumen;

import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Counters;

abstract class HistoryEventEmitter {
  static final private Log LOG = LogFactory.getLog(HistoryEventEmitter.class);

  abstract List<SingleEventEmitter> nonFinalSEEs();

  abstract List<SingleEventEmitter> finalSEEs();

  protected HistoryEventEmitter() {
    // no code
  }

  enum PostEmitAction {
    NONE, REMOVE_HEE
  };

  final Pair<Queue<HistoryEvent>, PostEmitAction> emitterCore(ParsedLine line,
      String name) {
    Queue<HistoryEvent> results = new LinkedList<HistoryEvent>();
    PostEmitAction removeEmitter = PostEmitAction.NONE;
    for (SingleEventEmitter see : nonFinalSEEs()) {
      HistoryEvent event = see.maybeEmitEvent(line, name, this);
      if (event != null) {
        results.add(event);
      }
    }
    for (SingleEventEmitter see : finalSEEs()) {
      HistoryEvent event = see.maybeEmitEvent(line, name, this);
      if (event != null) {
        results.add(event);
        removeEmitter = PostEmitAction.REMOVE_HEE;
        break;
      }
    }
    return new Pair<Queue<HistoryEvent>, PostEmitAction>(results, removeEmitter);
  }

  protected static Counters maybeParseCounters(String counters) {
    try {
      return parseCounters(counters);
    } catch (ParseException e) {
      LOG.warn("The counter string, \"" + counters + "\" is badly formatted.");
      return null;
    }
  }

  protected static Counters parseCounters(String counters)
      throws ParseException {
    if (counters == null) {
      LOG.warn("HistoryEventEmitters: null counter detected:");
      return null;
    }

    counters = counters.replace("\\.", "\\\\.");
    counters = counters.replace("\\\\(", "\\(");
    counters = counters.replace("\\\\)", "\\)");
    counters = counters.replace("\\\\[", "\\[");
    counters = counters.replace("\\\\]", "\\]");

    org.apache.hadoop.mapred.Counters depForm =
        org.apache.hadoop.mapred.Counters.fromEscapedCompactString(counters);

    return new Counters(depForm);
  }
}
