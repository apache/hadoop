/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Main class that invokes the FS-&gt;CS converter.
 *
 */
@SuppressWarnings("checkstyle:hideutilityclassconstructor")
public class FSConfigToCSConfigConverterMain {
  private static final Logger LOG =
      LoggerFactory.getLogger(FSConfigToCSConfigConverterMain.class);
  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");

  public static void main(String[] args) {
    try {
      FSConfigToCSConfigArgumentHandler fsConfigConversionArgumentHandler =
          new FSConfigToCSConfigArgumentHandler();
      int exitCode =
          fsConfigConversionArgumentHandler.parseAndConvert(args);
      if (exitCode != 0) {
        LOG.error(FATAL,
            "Error while starting FS configuration conversion, " +
                "see previous error messages for details!");
      }

      System.exit(exitCode);
    } catch (Throwable t) {
      LOG.error(FATAL,
          "Error while starting FS configuration conversion!", t);
      System.exit(-1);
    }
  }
}
