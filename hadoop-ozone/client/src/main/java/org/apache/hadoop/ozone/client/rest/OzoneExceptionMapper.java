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

package org.apache.hadoop.ozone.client.rest;


import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Class the represents various errors returned by the
 *  Object Layer.
 */
public class OzoneExceptionMapper implements ExceptionMapper<OzoneException> {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneExceptionMapper.class);

  @Override
  public Response toResponse(OzoneException exception) {
    LOG.debug("Returning exception. ex: {}", exception.toJsonString());
    MDC.clear();
    return Response.status((int)exception.getHttpCode())
      .entity(exception.toJsonString()).build();
  }

}
