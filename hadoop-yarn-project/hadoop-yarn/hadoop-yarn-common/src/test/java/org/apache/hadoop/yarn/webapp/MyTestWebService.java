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
 * Unless required by joblicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.webapp;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import jakarta.inject.Singleton;

import org.apache.hadoop.http.JettyUtils;

@Singleton
@Path("/ws/v1/test")
public class MyTestWebService {
  @GET
  @Produces({ MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public MyInfo get() {
    return new MyInfo();
  }

  @XmlRootElement(name = "myInfo")
  @XmlAccessorType(XmlAccessType.FIELD)
  static class MyInfo {
    public MyInfo() {

    }
  }
}