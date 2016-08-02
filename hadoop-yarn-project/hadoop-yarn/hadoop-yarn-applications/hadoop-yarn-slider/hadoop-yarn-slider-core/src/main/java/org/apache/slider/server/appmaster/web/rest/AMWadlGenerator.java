/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest;

import com.sun.jersey.server.wadl.ApplicationDescription;
import com.sun.jersey.server.wadl.WadlGenerator;
import com.sun.jersey.server.wadl.WadlGeneratorImpl;
import com.sun.research.ws.wadl.Application;
import com.sun.research.ws.wadl.Resource;
import com.sun.research.ws.wadl.Resources;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class AMWadlGenerator extends WadlGeneratorImpl {
  @Override
  /**
   * This method is called once the WADL application has been assembled, so it
   * affords an opportunity to edit the resources presented by the WADL.  In
   * this case, we're removing the internal "/agents" resources.
   */
  public void attachTypes(ApplicationDescription egd) {
    super.attachTypes(egd);

    Application application = egd.getApplication();
    List<Resources> resources = application.getResources();

    for (Resources appResources : resources) {
      List<Resource> resourceList = appResources.getResource();
      for (Resource appResource : resourceList) {
        String path = appResource.getPath();
        if (RestPaths.SLIDER_CONTEXT_ROOT.equals(path)) {
          List<Object> sliderResources = appResource.getMethodOrResource();
          Iterator<Object> itor = sliderResources.iterator();
          while (itor.hasNext()) {
            Object sliderRes = itor.next();
            if (sliderRes instanceof Resource) {
              Resource res = (Resource) sliderRes;
              if (RestPaths.SLIDER_SUBPATH_AGENTS.equals(res.getPath())) {
                // assuming I'll get a list modification issue if I remove at this
                // point
                itor.remove();
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void setWadlGeneratorDelegate(WadlGenerator delegate) {
    // do nothing
  }
}
