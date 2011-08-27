/*
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * Superclass for any type that maps to a potentially application-level query.
 * (e.g. Put, Get, Delete, Scan, Next, etc.)
 * Contains methods for exposure to logging and debugging tools.
 */
public abstract class Operation {
  // TODO make this configurable
  private static final int DEFAULT_MAX_COLS = 5;

  /**
   * Produces a Map containing a fingerprint which identifies the type and 
   * the static schema components of a query (i.e. column families)
   * @return a map containing fingerprint information (i.e. column families)
   */
  public abstract Map<String, Object> getFingerprint();

  /**
   * Produces a Map containing a summary of the details of a query 
   * beyond the scope of the fingerprint (i.e. columns, rows...)
   * @param maxCols a limit on the number of columns output prior to truncation
   * @return a map containing parameters of a query (i.e. rows, columns...)
   */
  public abstract Map<String, Object> toMap(int maxCols);

  /**
   * Produces a Map containing a full summary of a query.
   * @return a map containing parameters of a query (i.e. rows, columns...)
   */
  public Map<String, Object> toMap() {
    return toMap(DEFAULT_MAX_COLS);
  }

  /**
   * Produces a JSON object for fingerprint and details exposure in a
   * parseable format.
   * @param maxCols a limit on the number of columns to include in the JSON
   * @return a JSONObject containing this Operation's information, as a string
   */
  public String toJSON(int maxCols) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(toMap(maxCols));
  }

  /**
   * Produces a JSON object sufficient for description of a query
   * in a debugging or logging context.
   * @return the produced JSON object, as a string
   */
  public String toJSON() throws IOException {
    return toJSON(DEFAULT_MAX_COLS);
  }

  /**
   * Produces a string representation of this Operation. It defaults to a JSON
   * representation, but falls back to a string representation of the 
   * fingerprint and details in the case of a JSON encoding failure.
   * @param maxCols a limit on the number of columns output in the summary
   * prior to truncation
   * @return a JSON-parseable String
   */
  public String toString(int maxCols) {
    /* for now this is merely a wrapper from producing a JSON string, but 
     * toJSON is kept separate in case this is changed to be a less parsable
     * pretty printed representation.
     */
    try {
      return toJSON(maxCols);
    } catch (IOException ioe) {
      return toMap(maxCols).toString();
    }
  }

  /**
   * Produces a string representation of this Operation. It defaults to a JSON
   * representation, but falls back to a string representation of the
   * fingerprint and details in the case of a JSON encoding failure.
   * @return String
   */
  @Override
  public String toString() {
    return toString(DEFAULT_MAX_COLS);
  }
}

