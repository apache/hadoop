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

package org.apache.hadoop.chukwa.hicc;

import java.net.*;
import java.io.*;
import org.json.*;

public class JSONLoader {
    public JSONArray jsonData;
    static public String getContents(String source) {
        //...checks on aFile are elided
        StringBuffer contents = new StringBuffer();

        try {
          //use buffering, reading one line at a time
          //FileReader always assumes default encoding is OK!
	  URL yahoo = new URL(source);
	  BufferedReader in = new BufferedReader(
				new InputStreamReader(
				yahoo.openStream()));

	  String inputLine;

	  while ((inputLine = in.readLine()) != null) {
                contents.append(inputLine);
                contents.append(System.getProperty("line.separator"));
          }
          in.close();
        } catch (IOException ex){
          ex.printStackTrace();
        }

        return contents.toString();
    }

    public JSONLoader(String source) {
        String buffer = getContents(source);
        try {
            JSONObject rows = new JSONObject(buffer);
            jsonData = new JSONArray((String)rows.get("rows").toString());
        } catch (JSONException e) {
        }
    }

    public String getTS(int i) {
        String ts = null;
        try {
            ts = ((JSONObject)((JSONArray)jsonData).get(i)).get("ts").toString();
        } catch (JSONException e) {
        }
        return ts; 
    }
    
    public String getTags(int i) {
        String tags = null;
        try {
            tags = ((JSONObject)((JSONArray)jsonData).get(i)).get("tags").toString();
        } catch (JSONException e) {
        }
        return tags;
    }

    public String getValue(int i) {
        String value = null;
        try {
            value = ((JSONObject)((JSONArray)jsonData).get(i)).get("value").toString();
        } catch (JSONException e) {
        }
        return value;
    }

    public int length() {
        return ((JSONArray)jsonData).length();
    }
}
