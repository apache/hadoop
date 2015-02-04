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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.util.LineReader;

/**
 * {@link JobHistoryParser} to parse job histories for hadoop 0.20 (META=1).
 */
public class Hadoop20JHParser implements JobHistoryParser {
  final LineReader reader;

  static final String endLineString = " .";
  static final int internalVersion = 1;

  /**
   * Can this parser parse the input?
   * 
   * @param input
   * @return Whether this parser can parse the input.
   * @throws IOException
   * 
   *           We will deem a stream to be a good 0.20 job history stream if the
   *           first line is exactly "Meta VERSION=\"1\" ."
   */
  public static boolean canParse(InputStream input) throws IOException {
    try {
      LineReader reader = new LineReader(input);

      Text buffer = new Text();

      return reader.readLine(buffer) != 0
          && buffer.toString().equals("Meta VERSION=\"1\" .");
    } catch (EOFException e) {
      return false;
    }
  }

  public Hadoop20JHParser(InputStream input) throws IOException {
    super();

    reader = new LineReader(input);
  }

  public Hadoop20JHParser(LineReader reader) throws IOException {
    super();
    this.reader = reader;
  }

  Map<String, HistoryEventEmitter> liveEmitters =
      new HashMap<String, HistoryEventEmitter>();
  Queue<HistoryEvent> remainingEvents = new LinkedList<HistoryEvent>();

  enum LineType {
    JOB("Job", "JOBID") {
      HistoryEventEmitter createEmitter() {
        return new Job20LineHistoryEventEmitter();
      }
    },

    TASK("Task", "TASKID") {
      HistoryEventEmitter createEmitter() {
        return new Task20LineHistoryEventEmitter();
      }
    },

    MAP_ATTEMPT("MapAttempt", "TASK_ATTEMPT_ID") {
      HistoryEventEmitter createEmitter() {
        return new MapAttempt20LineHistoryEventEmitter();
      }
    },

    REDUCE_ATTEMPT("ReduceAttempt", "TASK_ATTEMPT_ID") {
      HistoryEventEmitter createEmitter() {
        return new ReduceAttempt20LineHistoryEventEmitter();
      }
    };

    private LogRecordType type;
    private String name;

    LineType(String s, String name) {
      type = LogRecordType.intern(s);
      this.name = name;
    }

    LogRecordType recordType() {
      return type;
    }

    String getName(ParsedLine line) {
      return line.get(name);
    }

    abstract HistoryEventEmitter createEmitter();

    static LineType findLineType(LogRecordType lrt) {
      for (LineType lt : LineType.values()) {
        if (lt.type == lrt) {
          return lt;
        }
      }

      return null;
    }
  }

  @Override
  public HistoryEvent nextEvent() {
    try {
      while (remainingEvents.isEmpty()) {
        ParsedLine line = new ParsedLine(getFullLine(), internalVersion);
        LineType type = LineType.findLineType(line.getType());
        if (type == null) {
          continue;
        }
        String name = type.getName(line);
        HistoryEventEmitter emitter = findOrMakeEmitter(name, type);
        Pair<Queue<HistoryEvent>, HistoryEventEmitter.PostEmitAction> pair =
            emitter.emitterCore(line, name);
        if (pair.second() == HistoryEventEmitter.PostEmitAction.REMOVE_HEE) {
          liveEmitters.remove(name);
        }
        remainingEvents = pair.first();
      }
      return remainingEvents.poll();
    } catch (EOFException e) {
      return null;
    } catch (IOException e) {
      return null;
    }
  }

  HistoryEventEmitter findOrMakeEmitter(String name, LineType type) {
    HistoryEventEmitter result = liveEmitters.get(name);
    if (result == null) {
      result = type.createEmitter();
      liveEmitters.put(name, result);
    }
    return result;
  }

  private String getOneLine() throws IOException {
    Text resultText = new Text();

    if (reader.readLine(resultText) == 0) {
      throw new EOFException("apparent bad line");
    }

    return resultText.toString();
  }

  private String getFullLine() throws IOException {
    String line = getOneLine();

    while (line.length() < endLineString.length()) {
      line = getOneLine();
    }

    if (line.endsWith(endLineString)) {
      return line;
    }

    StringBuilder sb = new StringBuilder(line);

    String addedLine;

    do {
      addedLine = getOneLine();
      sb.append("\n");
      sb.append(addedLine);
    } while (addedLine.length() < endLineString.length()
        || !endLineString.equals(addedLine.substring(addedLine.length()
            - endLineString.length())));

    return sb.toString();
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

}
