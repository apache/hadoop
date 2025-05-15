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

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class is merely a wrapper for {@link Response}. Given that the
 * entity input stream of {@link Response} can be read only once by
 * default and for some tests it is convenient to read the input stream many
 * times, this class hides the details of how to do that and prevents
 * unnecessary code duplication in tests.
 */
public class BufferedClientResponse {
  private final Response response;

  public BufferedClientResponse(Response response) {
    response.bufferEntity();
    this.response = response;
  }

  public <T> T getEntity(Class<T> clazz) {
    try {
      response.readEntity(InputStream.class).reset();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return response.readEntity(clazz);
  }

  public MediaType getType() {
    return response.getMediaType();
  }

  public int getStatus() {
    return response.getStatus();
  }

  public Response.StatusType getStatusInfo() {
    return response.getStatusInfo();
  }
}
