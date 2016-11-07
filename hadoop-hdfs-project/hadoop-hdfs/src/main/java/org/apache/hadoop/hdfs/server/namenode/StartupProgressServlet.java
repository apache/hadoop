/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Servlet that provides a JSON representation of the namenode's current startup
 * progress.
 */
@InterfaceAudience.Private
@SuppressWarnings("serial")
public class StartupProgressServlet extends DfsServlet {

  private static final String COUNT = "count";
  private static final String ELAPSED_TIME = "elapsedTime";
  private static final String FILE = "file";
  private static final String NAME = "name";
  private static final String DESC = "desc";
  private static final String PERCENT_COMPLETE = "percentComplete";
  private static final String PHASES = "phases";
  private static final String SIZE = "size";
  private static final String STATUS = "status";
  private static final String STEPS = "steps";
  private static final String TOTAL = "total";

  public static final String PATH_SPEC = "/startupProgress";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    resp.setContentType("application/json; charset=UTF-8");
    StartupProgress prog = NameNodeHttpServer.getStartupProgressFromContext(
      getServletContext());
    StartupProgressView view = prog.createView();
    JsonGenerator json = new JsonFactory().createGenerator(resp.getWriter());
    try {
      json.writeStartObject();
      json.writeNumberField(ELAPSED_TIME, view.getElapsedTime());
      json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete());
      json.writeArrayFieldStart(PHASES);

      for (Phase phase: view.getPhases()) {
        json.writeStartObject();
        json.writeStringField(NAME, phase.getName());
        json.writeStringField(DESC, phase.getDescription());
        json.writeStringField(STATUS, view.getStatus(phase).toString());
        json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase));
        json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase));
        writeStringFieldIfNotNull(json, FILE, view.getFile(phase));
        writeNumberFieldIfDefined(json, SIZE, view.getSize(phase));
        json.writeArrayFieldStart(STEPS);

        for (Step step: view.getSteps(phase)) {
          json.writeStartObject();
          StepType type = step.getType();
          if (type != null) {
            json.writeStringField(NAME, type.getName());
            json.writeStringField(DESC, type.getDescription());
          }
          json.writeNumberField(COUNT, view.getCount(phase, step));
          writeStringFieldIfNotNull(json, FILE, step.getFile());
          writeNumberFieldIfDefined(json, SIZE, step.getSize());
          json.writeNumberField(TOTAL, view.getTotal(phase, step));
          json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase,
            step));
          json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase, step));
          json.writeEndObject();
        }

        json.writeEndArray();
        json.writeEndObject();
      }

      json.writeEndArray();
      json.writeEndObject();
    } finally {
      IOUtils.cleanup(LOG, json);
    }
  }

  /**
   * Writes a JSON number field only if the value is defined.
   * 
   * @param json JsonGenerator to receive output
   * @param key String key to put
   * @param value long value to put
   * @throws IOException if there is an I/O error
   */
  private static void writeNumberFieldIfDefined(JsonGenerator json, String key,
      long value) throws IOException {
    if (value != Long.MIN_VALUE) {
      json.writeNumberField(key, value);
    }
  }

  /**
   * Writes a JSON string field only if the value is non-null.
   * 
   * @param json JsonGenerator to receive output
   * @param key String key to put
   * @param value String value to put
   * @throws IOException if there is an I/O error
   */
  private static void writeStringFieldIfNotNull(JsonGenerator json, String key,
      String value) throws IOException {
    if (value != null) {
      json.writeStringField(key, value);
    }
  }
}
