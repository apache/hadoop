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

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class RestActionStop {
  private static final Logger log =
      LoggerFactory.getLogger(RestActionStop.class);

  private final WebAppApi slider;
  
  public RestActionStop(WebAppApi slider) {
    this.slider = slider;
  }
  
  public StopResponse stop(HttpServletRequest request, UriInfo uriInfo, String body) {
    String verb = request.getMethod();
    log.info("Ping {}", verb);
    StopResponse response = new StopResponse();
    response.verb = verb;
    long time = System.currentTimeMillis();
    String text = 
        String.format(Locale.ENGLISH,
            "Stopping action %s received at %tc",
            verb, time);
    response.text = text;
    log.info(text);
    ActionStopSlider stopSlider =
        new ActionStopSlider(text,
            1000,
            TimeUnit.MILLISECONDS,
            LauncherExitCodes.EXIT_SUCCESS,
            FinalApplicationStatus.SUCCEEDED,
            text);
    log.info("SliderAppMasterApi.stopCluster: {}", stopSlider);
    slider.getQueues().schedule(stopSlider);
    
    return response;
  }
}
