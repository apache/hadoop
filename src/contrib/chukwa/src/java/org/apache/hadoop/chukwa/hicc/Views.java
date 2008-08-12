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

import java.io.*;
import java.util.*;
import org.json.*;

public class Views {
    public JSONArray viewsData;
    private String path = System.getProperty("catalina.home")+"/webapps/hicc/views/workspace_view_list.cache";
    static public String getContents(File aFile) {
        //...checks on aFile are elided
        StringBuffer contents = new StringBuffer();

        try {
          //use buffering, reading one line at a time
          //FileReader always assumes default encoding is OK!
          BufferedReader input =  new BufferedReader(new FileReader(aFile));
          try {
             String line = null; //not declared within while loop
             /*
              * readLine is a bit quirky :
              * it returns the content of a line MINUS the newline.
              * it returns null only for the END of the stream.
              * it returns an empty String if two newlines appear in a row.
              */
             while (( line = input.readLine()) != null){
                contents.append(line);
                contents.append(System.getProperty("line.separator"));
             }
          } finally {
             input.close();
          }
        }
          catch (IOException ex){
          ex.printStackTrace();
        }

        return contents.toString();
    }
    
    public Views() {
        File aFile = new File(path);
        String buffer = getContents(aFile);
        try {
            viewsData = new JSONArray(buffer);
        } catch (JSONException e) {
        }
    }

    public String getOwner(int i) {
        String owner = null;
        try {
            owner = ((JSONObject)((JSONArray)viewsData).get(i)).get("owner").toString();
        } catch (JSONException e) {
        }
        return owner;
    }
    
    public Iterator getPermission(int i) {
        Iterator permission = null;
        try {
            permission = ((JSONObject)((JSONObject)((JSONArray)viewsData).get(i)).get("permission")).keys();
        } catch (JSONException e) {
        }
        return permission;
    }
    
    public String getReadPermission(int i, String who) {
        String read = null;
        try {
            read = ((JSONObject)((JSONObject)((JSONObject)((JSONArray)viewsData).get(i)).get("permission")).get(who)).get("read").toString();
        } catch (JSONException e) {
        }
        return read;
    }

    public String getWritePermission(int i, String who) {
        String write = null;
        try {
            write = ((JSONObject)((JSONObject)((JSONObject)((JSONArray)viewsData).get(i)).get("permission")).get(who)).get("write").toString();
        } catch (JSONException e) {
        }
        return write;
    }
    
    public String getDescription(int i) {
        String description = null;
        try {
            description = ((JSONObject)((JSONArray)viewsData).get(i)).get("description").toString();
        } catch (JSONException e) {
        }
        return description;
    }

    public String getKey(int i) {
        String key = null;
        try {
            key = ((JSONObject)((JSONArray)viewsData).get(i)).get("key").toString();
        } catch (JSONException e) {
        }
        return key;
    }

    public int length() {
        return ((JSONArray)viewsData).length();
    }
}
