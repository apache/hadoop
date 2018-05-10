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
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.ozone.client.rest.OzoneException;

/**
 * This exception is thrown by the Ozone Clients.
 */
public class OzoneClientException extends OzoneException {
  /**
   * Constructor that allows the shortMessage.
   *
   * @param shortMessage Short Message
   */
  public OzoneClientException(String shortMessage) {
    super(0, shortMessage, shortMessage);
  }

  /**
   * Constructor that allows a shortMessage and an exception.
   *
   * @param shortMessage short message
   * @param ex exception
   */
  public OzoneClientException(String shortMessage, Exception ex) {
    super(0, shortMessage, shortMessage, ex);
  }

  /**
   * Constructor that allows the shortMessage and a longer message.
   *
   * @param shortMessage Short Message
   * @param message long error message
   */
  public OzoneClientException(String shortMessage, String message) {
    super(0, shortMessage, message);
  }
}
