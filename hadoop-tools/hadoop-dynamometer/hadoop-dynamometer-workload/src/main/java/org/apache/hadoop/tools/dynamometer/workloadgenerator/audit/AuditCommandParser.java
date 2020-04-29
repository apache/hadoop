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
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * This interface represents a pluggable command parser. It will accept in one
 * line of {@link Text} input at a time and return an {@link AuditReplayCommand}
 * which represents the input text. Each input line should produce exactly one
 * command.
 */
public interface AuditCommandParser {

  /**
   * Initialize this parser with the given configuration. Guaranteed to be
   * called prior to any calls to {@link #parse(Text, Function)}.
   *
   * @param conf The Configuration to be used to set up this parser.
   * @throws IOException if error on initializing a parser.
   */
  void initialize(Configuration conf) throws IOException;

  /**
   * Convert a line of input into an {@link AuditReplayCommand}. Since
   * {@link AuditReplayCommand}s store absolute timestamps, relativeToAbsolute
   * can be used to convert relative timestamps (i.e., milliseconds elapsed
   * between the start of the audit log and this command) into absolute
   * timestamps.
   *
   * @param inputLine Single input line to convert.
   * @param relativeToAbsolute Function converting relative timestamps
   *                           (in milliseconds) to absolute timestamps
   *                           (in milliseconds).
   * @return A command representing the input line.
   * @throws IOException if error on parsing.
   */
  AuditReplayCommand parse(Text inputLine,
      Function<Long, Long> relativeToAbsolute) throws IOException;

}
