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

package org.apache.hadoop.yarn.webapp;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.classification.InterfaceAudience;

/*
 * Created our own NotFoundException because com.sun.jersey.api.NotFoundException
 * sets the Response and therefore won't be handled by the GenericExceptionhandler
 * to fill in correct response.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class NotFoundException extends WebApplicationException {

  private static final long serialVersionUID = 1L;

  public NotFoundException() {
    super(Status.NOT_FOUND);
  }

  public NotFoundException(java.lang.Throwable cause) {
    super(cause, Status.NOT_FOUND);
  }

  public NotFoundException(String msg) {
    super(new Exception(msg), Status.NOT_FOUND);
  }
}
