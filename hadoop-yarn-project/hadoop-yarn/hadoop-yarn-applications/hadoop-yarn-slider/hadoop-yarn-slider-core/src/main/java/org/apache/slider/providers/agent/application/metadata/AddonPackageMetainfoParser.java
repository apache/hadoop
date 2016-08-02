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

import org.apache.commons.digester.Digester;

/**
 *
 */
public class AddonPackageMetainfoParser extends AbstractMetainfoParser {

  protected void composeSchema(Digester digester) {
    digester.addObjectCreate("metainfo", Metainfo.class);
    digester.addBeanPropertySetter("metainfo/schemaVersion");

    digester.addObjectCreate("*/applicationPackage", ApplicationPackage.class);
    digester.addBeanPropertySetter("*/applicationPackage/name");
    digester.addBeanPropertySetter("*/applicationPackage/comment");
    digester.addBeanPropertySetter("*/applicationPackage/version");

    digester.addObjectCreate("*/component", ComponentsInAddonPackage.class);
    digester.addBeanPropertySetter("*/component/name");
    digester.addSetNext("*/component", "addComponent");

    digester.addObjectCreate("*/commandScript", CommandScript.class);
    digester.addBeanPropertySetter("*/commandScript/script");
    digester.addBeanPropertySetter("*/commandScript/scriptType");
    digester.addBeanPropertySetter("*/commandScript/timeout");
    digester.addSetNext("*/commandScript", "addCommandScript");

    digester.addObjectCreate("*/configFile", ConfigFile.class);
    digester.addBeanPropertySetter("*/configFile/type");
    digester.addBeanPropertySetter("*/configFile/fileName");
    digester.addBeanPropertySetter("*/configFile/dictionaryName");
    digester.addSetNext("*/configFile", "addConfigFile");

    digester.addSetRoot("*/applicationPackage", "setApplicationPackage");
  }
}
