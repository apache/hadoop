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

package org.apache.hadoop.lib.wsrs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

@InterfaceAudience.Private
public class ExceptionProvider implements ExceptionMapper<Throwable> {
  private static Logger LOG = LoggerFactory.getLogger(ExceptionProvider.class);

  private static final String ENTER = System.getProperty("line.separator");

  protected Response createResponse(Response.Status status, Throwable throwable) {
    return HttpExceptionUtils.createJerseyExceptionResponse(status, throwable);
  }

  protected String getOneLineMessage(Throwable throwable) {
    String message = throwable.getMessage();
    if (message != null) {
      int i = message.indexOf(ENTER);
      if (i > -1) {
        message = message.substring(0, i);
      }
    }
    return message;
  }

  protected void log(Response.Status status, Throwable throwable) {
    LOG.debug("{}", throwable.getMessage(), throwable);
  }

  @Override
  public Response toResponse(Throwable throwable) {
    return createResponse(Response.Status.BAD_REQUEST, throwable);
  }

}
