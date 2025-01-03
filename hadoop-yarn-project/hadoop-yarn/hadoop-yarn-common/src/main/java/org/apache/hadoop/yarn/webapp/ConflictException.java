/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.webapp;

import org.apache.hadoop.classification.InterfaceAudience;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class ConflictException extends WebApplicationException {
  private static final long serialVersionUID = 1L;

  public ConflictException() {
    super(Response.Status.CONFLICT);
  }

  public ConflictException(java.lang.Throwable cause) {
    super(cause, Response.Status.CONFLICT);
  }

  public ConflictException(String msg) {
    super(new Exception(msg), Response.Status.CONFLICT);
  }
}
