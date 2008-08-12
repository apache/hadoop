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
import javax.servlet.*;
import javax.servlet.http.*;
import java.sql.*;
import org.json.*;

public class Workspace extends HttpServlet {

    private String path=System.getProperty("catalina.home")+"/webapps/hicc";
    private JSONObject hash=new JSONObject();
    private String user="admin";

    public void doGet(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        String method = request.getParameter("method");
        if(method.equals("get_views_list")) {
            getViewsList(request, response);
        }
        if(method.equals("get_view")) {
            getView(request, response);
        }
        if(method.equals("save_view")) {
            saveView(request, response);
        }
        if(method.equals("change_view_info")) {
            changeViewInfo(request, response);
        }
        if(method.equals("get_widget_list")) {
            getWidgetList(request, response);
        }
        if(method.equals("clone_view")) {
            cloneView(request, response);
        }
        if(method.equals("delete_view")) {
            deleteView(request, response);
        }
    }

    public void doPost(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        doGet(request, response);
    }

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

    public void setContents(String fName, String buffer) {
        try {
            FileWriter fstream = new FileWriter(fName);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(buffer);
            out.close();
        } catch (Exception e) {
            System.err.println("Error: "+e.getMessage());
        }
    }

    public void cloneView(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String name = request.getParameter("name");
        String template = request.getParameter("clone_name");
        File aFile = new File(path+"/views/"+template);
        String config = getContents(aFile);
        int i=0;
        boolean check=true;
        while(check) {
            String tmpName = name;
            if(i>0) {
                tmpName = name + i;
            }
            File checkFile = new File(path+"/views/"+tmpName+".view");
            check = checkFile.exists();
            if(!check) {
                name =tmpName;
            }
            i=i+1;
        }
        setContents(path+"/views/"+name+".view",config);
        File deleteCache = new File(path+"/views/workspace_view_list.cache");
        deleteCache.delete();
        genViewCache(path+"/views");
        aFile = new File(path+"/views/workspace_view_list.cache");
        String viewsCache = getContents(aFile);
        out.println(viewsCache);
    }
    public void deleteView(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String name = request.getParameter("name");
        File aFile = new File(path+"/views/"+name+".view");
        aFile.delete();
        File deleteCache = new File(path+"/views/workspace_view_list.cache");
        deleteCache.delete();
        genViewCache(path+"/views");
    }
    public void getViewsList(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String format = request.getParameter("format");
        File aFile = new File(path+"/views/workspace_view_list.cache");
        String viewsCache = getContents(aFile);
        out.println(viewsCache);
    }
    public void getView(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String id = request.getParameter("id");
        genViewCache(path+"/views");
        File aFile = new File(path+"/views/"+id+".view");
        String view = getContents(aFile);
        out.println(view);
    }
    public void changeViewInfo(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String id = request.getParameter("name");
        String config = request.getParameter("config");
        try {
            JSONObject jt = new JSONObject(config);
            File aFile = new File(path+"/views/"+id+".view");
            String original = getContents(aFile);
            JSONObject updateObject = new JSONObject(original);
            updateObject.put("description",jt.get("description"));
            setContents(path+"/views/"+id+".view",updateObject.toString());
            File deleteCache = new File(path+"/views/workspace_view_list.cache");
            deleteCache.delete();
            genViewCache(path+"/views");
            out.println("Workspace is stored successfully.");
        } catch(JSONException e) {
            out.println("Workspace store failed.");
        }
    }
    public void saveView(HttpServletRequest request,
                      HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String id = request.getParameter("name");
        String config = request.getParameter("config");
        File aFile = new File(path+"/views/"+id+".view");
        setContents(path+"/views/"+id+".view",config);
        out.println("Workspace is stored successfully.");
    }
    public void getWidgetList(HttpServletRequest request,
                             HttpServletResponse response)
        throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();
        String format = request.getParameter("format");
        genWidgetCache(path+"/descriptors");
        File aFile = new File(path+"/descriptors/workspace_plugin.cache");
        String viewsCache = getContents(aFile);
        out.println(viewsCache);
    }
    private void genViewCache(String source) {
        File cacheFile = new File(source+"/workspace_view_list.cache");
        if(! cacheFile.exists()) {
            File dir = new File(source);
            File[] filesWanted = dir.listFiles(
                new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                    return name.endsWith(".view");
               }
            });
            JSONObject[] cacheGroup = new JSONObject[filesWanted.length];
            for(int i=0; i< filesWanted.length; i++) {
               String buffer = getContents(filesWanted[i]);
               try {
                   JSONObject jt = new JSONObject(buffer);
                   String fn = filesWanted[i].getName();
                   jt.put("key", fn.substring(0,(fn.length()-5)));
                   cacheGroup[i] = jt;
               } catch (Exception e) {
               }
            }
            String viewList = convertObjectsToViewList(cacheGroup);
            setContents(source+"/workspace_view_list.cache", viewList);
        }
    }
    public String convertObjectsToViewList(JSONObject[] objArray) {
        JSONArray jsonArr = new JSONArray();
        JSONObject permission = new JSONObject();
        JSONObject user = new JSONObject();
        try {
            permission.put("read",1);
            permission.put("modify",1);
            user.put("all",permission);
        } catch (Exception e) {
               System.err.println("JSON Exception: "+e.getMessage());
        }
        for(int i=0;i<objArray.length;i++) {
            try {
               JSONObject jsonObj = new JSONObject();
               jsonObj.put("key",objArray[i].get("key"));
               jsonObj.put("description",objArray[i].get("description"));
               jsonObj.put("owner","");
               jsonObj.put("permission",user);
               jsonArr.put(jsonObj);
           } catch (Exception e) {
               System.err.println("JSON Exception: "+e.getMessage());
           }
        }
        return jsonArr.toString();
    }
    private void genWidgetCache(String source) {
        File cacheFile = new File(source+"/workspace_plugin.cache");
        File cacheDir = new File(source);
        if(! cacheFile.exists() || cacheFile.lastModified()<cacheDir.lastModified()) {
            File dir = new File(source);
            File[] filesWanted = dir.listFiles(
                new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                    return name.endsWith(".descriptor");
               }
            });
            JSONObject[] cacheGroup = new JSONObject[filesWanted.length];
            for(int i=0; i< filesWanted.length; i++) {
               String buffer = getContents(filesWanted[i]);
               try {
                   JSONObject jt = new JSONObject(buffer);
                   cacheGroup[i] = jt;
               } catch (Exception e) {
               }
            }
            String widgetList = convertObjectsToWidgetList(cacheGroup);
            setContents(source+"/workspace_plugin.cache", widgetList);
        }
    }
    public String convertObjectsToWidgetList(JSONObject[] objArray) {
        JSONObject jsonObj = new JSONObject();
        JSONArray jsonArr = new JSONArray();
        for(int i=0;i<objArray.length;i++) {
            jsonArr.put(objArray[i]);
        }
        try {
            jsonObj.put("detail", jsonArr);
        } catch (Exception e) {
            System.err.println("JSON Exception: "+e.getMessage());
        }
        JSONObject tmpHash = new JSONObject();
        for(int i=0;i<objArray.length;i++) {
            try {
                String[] categoriesArray = objArray[i].get("categories").toString().split(",");
                hash = addToHash(hash,categoriesArray,objArray[i]); 
            } catch (JSONException e) {
                System.err.println("JSON Exception: "+e.getMessage());
            }
        }
        try {
            jsonObj.put("children",hash);
        } catch (Exception e) {
            System.err.println("JSON Exception: "+e.getMessage());
        }
        return jsonObj.toString();
    }
    public JSONObject addToHash(JSONObject hash, String[] categoriesArray, JSONObject obj) {
        JSONObject subHash=hash;
        for(int i=0;i<categoriesArray.length;i++) {
            String id = categoriesArray[i];
            if(i>=categoriesArray.length-1) {
                try {
                    subHash.put("leaf:"+obj.get("title"),obj.get("id"));
                } catch (Exception e) {
                    System.err.println("JSON Exception: "+e.getMessage());
                }
            } else {
                try {
                    subHash=subHash.getJSONObject("node:"+id);
                } catch (JSONException e) {
                    try {
                        JSONObject tmpHash = new JSONObject();
                        subHash.put("node:"+id, tmpHash);
                        subHash=tmpHash;
                    } catch (JSONException ex) {
                    }
                }
            }
        }
        return hash;
    }
    private JSONObject filterViewsByPermission(String userid, JSONObject viewArray) {
        return viewArray;
    }
}



