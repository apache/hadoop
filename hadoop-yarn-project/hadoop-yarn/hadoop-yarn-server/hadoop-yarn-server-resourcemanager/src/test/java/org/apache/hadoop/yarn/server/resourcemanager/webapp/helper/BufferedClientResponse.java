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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.helper;


import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * This class is merely a wrapper for {@link ClientResponse}. Given that the
 * entity input stream of {@link ClientResponse} can be read only once by
 * default and for some tests it is convenient to read the input stream many
 * times, this class hides the details of how to do that and prevents
 * unnecessary code duplication in tests.
 */
public class BufferedClientResponse {
  private ClientResponse response;

  public BufferedClientResponse(ClientResponse response) {
    response.bufferEntity();
    this.response = response;
  }

  public <T> T getEntity(Class<T> clazz)
          throws ClientHandlerException, UniformInterfaceException {
    try {
      response.getEntityInputStream().reset();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return response.getEntity(clazz);
  }

  public MediaType getType() {
    return response.getType();
  }

  public int getStatus() {
    return response.getStatus();
  }

  public Response.StatusType getStatusInfo() {
    return response.getStatusInfo();
  }
}
