/*
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

package org.apache.slider.server.appmaster.web.rest.application.actions;

import org.apache.slider.api.types.PingInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import java.util.Locale;

public class RestActionPing {
  private static final Logger log =
      LoggerFactory.getLogger(RestActionPing.class);

  public RestActionPing() {
  }
  
  public PingInformation ping(HttpServletRequest request, UriInfo uriInfo, String body) {
    String verb = request.getMethod();
    log.info("Ping {}", verb);
    PingInformation pingInformation = new PingInformation();
    pingInformation.time = System.currentTimeMillis();
    pingInformation.verb = verb;
    pingInformation.body = body;
    String text = 
        String.format(Locale.ENGLISH,
            "Ping verb %s received at %tc",
            verb, pingInformation.time);
    pingInformation.text = text;
    return pingInformation;
  }
}
