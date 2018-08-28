/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit;

import org.apache.logging.log4j.message.Message;

import java.util.Map;

/**
 * Defines audit message structure.
 */
public class AuditMessage implements Message {

  private String message;

  public AuditMessage(String user, String ip, String op,
      Map<String, String> params, String ret){

    this.message = String.format("user=%s ip=%s op=%s %s ret=%s",
                                  user, ip, op, params, ret);
  }

  @Override
  public String getFormattedMessage() {
    return message;
  }

  @Override
  public String getFormat() {
    return null;
  }

  @Override
  public Object[] getParameters() {
    return new Object[0];
  }

  @Override
  public Throwable getThrowable() {
    return null;
  }

  /**
   * Use when there are custom string to be added to default msg.
   * @param customMessage custom string
   */
  private void appendMessage(String customMessage) {
    this.message += customMessage;
  }
}
