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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;

/**
 * {@link JobHistoryParser} that parses JobHistory files.
 */
public class CurrentJHParser implements JobHistoryParser {
  private EventReader reader;

  private static class ForkedDataInputStream extends DataInputStream {
    ForkedDataInputStream(InputStream input) {
      super(input);
    }

    @Override
    public void close() {
      // no code
    }
  }

  /**
   * Can this parser parse the input?
   * 
   * @param input
   * @return Whether this parser can parse the input.
   * @throws IOException
   */
  public static boolean canParse(InputStream input) throws IOException {
    final DataInputStream in = new ForkedDataInputStream(input);

    try {
      final EventReader reader = new EventReader(in);

      try {
        reader.getNextEvent();
      } catch (IOException e) {
        return false;
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      return false;
    }

    return true;
  }

  public CurrentJHParser(InputStream input) throws IOException {
    reader = new EventReader(new DataInputStream(input));
  }

  @Override
  public HistoryEvent nextEvent() throws IOException {
    return reader.getNextEvent();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
