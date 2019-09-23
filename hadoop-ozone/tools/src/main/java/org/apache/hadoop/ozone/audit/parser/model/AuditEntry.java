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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit.parser.model;

/**
 * POJO used for ozone audit parser tool.
 */
public class AuditEntry {
  private String timestamp;
  private String level;
  private String logger;
  private String user;
  private String ip;
  private String op;
  private String params;
  private String result;
  private String exception;

  public AuditEntry(){}

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getLogger() {
    return logger;
  }

  public void setLogger(String logger) {
    this.logger = logger;
  }

  public String getOp() {
    return op;
  }

  public void setOp(String op) {
    this.op = op;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public String getResult() {
    return result;
  }

  public void setResult(String result) {
    this.result = result;
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception.trim();
  }

  public void appendException(String text){
    this.exception += "\n" + text.trim();
  }

  /**
   * Builder for AuditEntry.
   */
  public static class Builder {
    private String timestamp;
    private String level;
    private String logger;
    private String user;
    private String ip;
    private String op;
    private String params;
    private String result;
    private String exception;

    public Builder() {

    }

    public Builder setTimestamp(String ts){
      this.timestamp = ts;
      return this;
    }

    public Builder setLevel(String lvl){
      this.level = lvl;
      return this;
    }

    public Builder setLogger(String lgr){
      this.logger = lgr;
      return this;
    }

    public Builder setUser(String usr){
      this.user = usr;
      return this;
    }

    public Builder setIp(String ipAddress){
      this.ip = ipAddress;
      return this;
    }

    public Builder setOp(String operation){
      this.op = operation;
      return this;
    }

    public Builder setParams(String prms){
      this.params = prms;
      return this;
    }

    public Builder setResult(String res){
      this.result = res;
      return this;
    }

    public Builder setException(String exp){
      this.exception = exp;
      return this;
    }

    public AuditEntry build() {
      AuditEntry aentry = new AuditEntry();
      aentry.timestamp = this.timestamp;
      aentry.level = this.level;
      aentry.logger = this.logger;
      aentry.user = this.user;
      aentry.ip = this.ip;
      aentry.op = this.op;
      aentry.params = this.params;
      aentry.result = this.result;
      aentry.exception = this.exception;
      return aentry;
    }
  }
}
