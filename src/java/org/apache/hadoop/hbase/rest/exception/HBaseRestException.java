/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rest.exception;

import agilejson.TOJSON;

public class HBaseRestException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 8481585437124298646L;
  private Exception innerException;
  private String innerClass;
  private String innerMessage;

  public HBaseRestException() {

  }

  public HBaseRestException(Exception e) throws HBaseRestException {
    if (HBaseRestException.class.isAssignableFrom(e.getClass())) {
      throw ((HBaseRestException) e);
    }
    setInnerException(e);
    innerClass = e.getClass().toString();
    innerMessage = e.getMessage();
  }

  /**
   * @param message
   */
  public HBaseRestException(String message) {
    super(message);
    innerMessage = message;
  }

  public HBaseRestException(String message, Exception exception) {
    super(message, exception);
    setInnerException(exception);
    innerClass = exception.getClass().toString();
    innerMessage = message;
  }

  @TOJSON
  public String getInnerClass() {
    return this.innerClass;
  }

  @TOJSON
  public String getInnerMessage() {
    return this.innerMessage;
  }

  /**
   * @param innerException
   *          the innerException to set
   */
  public void setInnerException(Exception innerException) {
    this.innerException = innerException;
  }

  /**
   * @return the innerException
   */
  public Exception getInnerException() {
    return innerException;
  }
}
