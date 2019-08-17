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
  private Throwable throwable;

  private static final String MSG_PATTERN =
      "user=%s | ip=%s | op=%s %s | ret=%s";

  public AuditMessage(){

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
    return throwable;
  }

  /**
   * Use when there are custom string to be added to default msg.
   * @param customMessage custom string
   */
  private void appendMessage(String customMessage) {
    this.message += customMessage;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void setThrowable(Throwable throwable) {
    this.throwable = throwable;
  }

  /**
   * Builder class for AuditMessage.
   */
  public static class Builder {
    private Throwable throwable;
    private String user;
    private String ip;
    private String op;
    private Map<String, String> params;
    private String ret;

    public Builder(){

    }

    public Builder setUser(String usr){
      this.user = usr;
      return this;
    }

    public Builder atIp(String ipAddr){
      this.ip = ipAddr;
      return this;
    }

    public Builder forOperation(String operation){
      this.op = operation;
      return this;
    }

    public Builder withParams(Map<String, String> args){
      this.params = args;
      return this;
    }

    public Builder withResult(String result){
      this.ret = result;
      return this;
    }

    public Builder withException(Throwable ex){
      this.throwable = ex;
      return this;
    }

    public AuditMessage build(){
      AuditMessage auditMessage = new AuditMessage();
      auditMessage.message = String.format(MSG_PATTERN,
          this.user, this.ip, this.op, this.params, this.ret);
      auditMessage.throwable = this.throwable;
      return auditMessage;
    }
  }
}
