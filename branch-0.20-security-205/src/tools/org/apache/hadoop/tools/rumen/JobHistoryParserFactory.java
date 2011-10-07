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

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link JobHistoryParserFactory} is a singleton class that attempts to
 * determine the version of job history and return a proper parser.
 */
public class JobHistoryParserFactory {
  public static JobHistoryParser getParser(RewindableInputStream ris)
      throws IOException {
    for (VersionDetector vd : VersionDetector.values()) {
      boolean canParse = vd.canParse(ris);
      ris.rewind();
      if (canParse) {
        return vd.newInstance(ris);
      }
    }

    throw new IOException("No suitable parser.");
  }

  enum VersionDetector {
    Hadoop20() {

      @Override
      public boolean canParse(InputStream input) throws IOException {
        return Hadoop20JHParser.canParse(input);
      }

      @Override
      public JobHistoryParser newInstance(InputStream input) throws IOException {
        return new Hadoop20JHParser(input);
      }
    };

    abstract JobHistoryParser newInstance(InputStream input) throws IOException;

    abstract boolean canParse(InputStream input) throws IOException;
  }
}
