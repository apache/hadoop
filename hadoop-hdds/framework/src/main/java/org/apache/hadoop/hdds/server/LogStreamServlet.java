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
package org.apache.hadoop.hdds.server;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

/**
 * Servlet to stream the current logs to the response.
 */
public class LogStreamServlet extends HttpServlet {

  private static final String PATTERN = "%d [%p|%c|%C{1}] %m%n";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    WriterAppender appender =
        new WriterAppender(new PatternLayout(PATTERN), resp.getWriter());
    appender.setThreshold(Level.TRACE);

    try {
      Logger.getRootLogger().addAppender(appender);
      try {
        Thread.sleep(Integer.MAX_VALUE);
      } catch (InterruptedException e) {
        //interrupted
      }
    } finally {
      Logger.getRootLogger().removeAppender(appender);
    }
  }

}
