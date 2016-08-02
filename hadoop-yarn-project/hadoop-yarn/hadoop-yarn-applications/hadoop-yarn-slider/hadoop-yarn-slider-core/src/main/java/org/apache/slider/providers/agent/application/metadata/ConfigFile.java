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
 *
 */
public class ConfigFile implements Validate {
  String type;
  String fileName;
  String dictionaryName;

  public ConfigFile() {
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getDictionaryName() {
    return dictionaryName;
  }

  public void setDictionaryName(String dictionaryName) {
    this.dictionaryName = dictionaryName;
  }

  public void validate(String version) throws SliderException {

  }
}
