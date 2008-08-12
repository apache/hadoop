/*
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

package org.apache.hadoop.chukwa.inputtools.mdl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

public class DataConfig {
    private static Configuration config;
    final static String DATACONFIG = "mdl.xml";
    private Log log = LogFactory.getLog(DataConfig.class);
    
    public DataConfig(String path) {
        Path fileResource = new Path(path);
        config = new Configuration();
        config.addResource(fileResource);
    }
    public DataConfig() {
    	String dataConfig = System.getenv("DATACONFIG");
    	if(dataConfig==null) {
    		dataConfig=DATACONFIG;
    	}
    	log.debug("DATACONFIG="+dataConfig);
    	if(config==null)  {
    		try {
                Path fileResource = new Path(dataConfig);
                config = new Configuration();
                config.addResource(fileResource);
    		} catch (Exception e) {
    			log.debug("Error reading configuration file:"+dataConfig);
    		}
    	}
    }

    public String get(String key) {
        return config.get(key);
    }
    public void put(String key, String value) {
        config.set(key, value);
    }
    public Iterator<Map.Entry <String, String>> iterator() {
        return config.iterator();
    }
    public HashMap<String, String> startWith(String key) {
        HashMap<String, String> transformer = new HashMap<String, String>();
        Iterator<Map.Entry <String, String>> entries = config.iterator();
        while(entries.hasNext()) {
           String entry = entries.next().toString();
           if(entry.startsWith(key)) {
               String[] metrics = entry.split("=");
               transformer.put(metrics[0],metrics[1]);
           }
        }
        return transformer;
    }
}
