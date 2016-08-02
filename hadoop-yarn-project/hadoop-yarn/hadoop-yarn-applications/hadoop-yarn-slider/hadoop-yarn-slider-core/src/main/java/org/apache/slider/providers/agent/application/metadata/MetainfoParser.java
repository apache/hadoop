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
public class MetainfoParser extends AbstractMetainfoParser{
  
  protected void composeSchema(Digester digester){
    digester.addObjectCreate("metainfo", Metainfo.class);
    digester.addBeanPropertySetter("metainfo/schemaVersion");

    digester.addObjectCreate("*/application", Application.class);
    digester.addBeanPropertySetter("*/application/name");
    digester.addBeanPropertySetter("*/application/comment");
    digester.addBeanPropertySetter("*/application/version");
    digester.addBeanPropertySetter("*/application/exportedConfigs");

    digester.addObjectCreate("*/commandOrder", CommandOrder.class);
    digester.addBeanPropertySetter("*/commandOrder/command");
    digester.addBeanPropertySetter("*/commandOrder/requires");
    digester.addSetNext("*/commandOrder", "addCommandOrder");

    digester.addObjectCreate("*/exportGroup", ExportGroup.class);
    digester.addBeanPropertySetter("*/exportGroup/name");
    digester.addObjectCreate("*/export", Export.class);
    digester.addBeanPropertySetter("*/export/name");
    digester.addBeanPropertySetter("*/export/value");
    digester.addSetNext("*/export", "addExport");
    digester.addSetNext("*/exportGroup", "addExportGroup");

    digester.addObjectCreate("*/component", Component.class);
    digester.addBeanPropertySetter("*/component/name");
    digester.addBeanPropertySetter("*/component/category");
    digester.addBeanPropertySetter("*/component/publishConfig");
    digester.addBeanPropertySetter("*/component/minInstanceCount");
    digester.addBeanPropertySetter("*/component/maxInstanceCount");
    digester.addBeanPropertySetter("*/component/autoStartOnFailure");
    digester.addBeanPropertySetter("*/component/appExports");
    digester.addBeanPropertySetter("*/component/compExports");
    digester.addObjectCreate("*/componentExport", ComponentExport.class);
    digester.addBeanPropertySetter("*/componentExport/name");
    digester.addBeanPropertySetter("*/componentExport/value");
    digester.addSetNext("*/componentExport", "addComponentExport");
    digester.addSetNext("*/component", "addComponent");

    digester.addObjectCreate("*/commandScript", CommandScript.class);
    digester.addBeanPropertySetter("*/commandScript/script");
    digester.addBeanPropertySetter("*/commandScript/scriptType");
    digester.addBeanPropertySetter("*/commandScript/timeout");
    digester.addSetNext("*/commandScript", "addCommandScript");

    digester.addObjectCreate("*/command", ComponentCommand.class);
    digester.addBeanPropertySetter("*/command/exec");
    digester.addBeanPropertySetter("*/command/name");
    digester.addBeanPropertySetter("*/command/type");
    digester.addSetNext("*/command", "addCommand");

    digester.addObjectCreate("*/osSpecific", OSSpecific.class);
    digester.addBeanPropertySetter("*/osSpecific/osType");
    digester.addObjectCreate("*/osSpecific/packages/package", OSPackage.class);
    digester.addBeanPropertySetter("*/osSpecific/packages/package/type");
    digester.addBeanPropertySetter("*/osSpecific/packages/package/name");
    digester.addSetNext("*/osSpecific/packages/package", "addOSPackage");
    digester.addSetNext("*/osSpecific", "addOSSpecific");

    digester.addObjectCreate("*/application/packages/package", Package.class);
    digester.addBeanPropertySetter("*/application/packages/package/type");
    digester.addBeanPropertySetter("*/application/packages/package/name");
    digester.addSetNext("*/application/packages/package", "addPackage");

    digester.addObjectCreate("*/configFile", ConfigFile.class);
    digester.addBeanPropertySetter("*/configFile/type");
    digester.addBeanPropertySetter("*/configFile/fileName");
    digester.addBeanPropertySetter("*/configFile/dictionaryName");
    digester.addSetNext("*/configFile", "addConfigFile");

    digester.addSetRoot("*/application", "setApplication");
  }
}
