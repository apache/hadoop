/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.providers.agent.application.metadata;

import org.apache.slider.core.exceptions.SliderException;

/**
 * CommandScript that implements all component commands
 */
public class CommandScript implements Validate {
  String script;
  String scriptType;
  long timeout;

  public CommandScript() {

  }

  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }

  public String getScriptType() {
    return scriptType;
  }

  public void setScriptType(String scriptType) {
    this.scriptType = scriptType;
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"script\": ").append(script);
    sb.append(",\n\"scriptType\": ").append(scriptType);
    sb.append(",\n\"timeout\" :").append(timeout);
    sb.append('}');
    return sb.toString();
  }

  public void validate(String version) throws SliderException {
    Metainfo.checkNonNull(getScript(), "script", "commandScript");
    Metainfo.checkNonNull(getScriptType(), "scriptType", "commandScript");
  }
}
