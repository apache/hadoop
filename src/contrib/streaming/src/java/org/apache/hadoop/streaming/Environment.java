/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.streaming;

import java.io.*;
import java.util.*;

public class Environment extends Properties
{
   public Environment()
      throws IOException
   {
      // Extend this code to fit all operating
      // environments that you expect to run in

      String command = null;
      String OS = System.getProperty("os.name");
      if (OS.equals("Windows NT")) {
         command = "cmd /C set";
      } else if (OS.indexOf("ix") > -1 || OS.indexOf("inux") > -1) {
         command = "env";
      } else {
         // Add others here
      }

      if (command == null) {
         throw new RuntimeException("Operating system " + OS
            + " not supported by this class");
      }

      // Read the environment variables

      Process pid = Runtime.getRuntime().exec(command);
      BufferedReader in =
         new BufferedReader(
         new InputStreamReader(
         pid.getInputStream()));
      while(true) {
         String line = in.readLine();
         if (line == null)
            break;
         int p = line.indexOf("=");
         if (p != -1) {
            String name = line.substring(0, p);
            String value = line.substring(p+1);
            setProperty(name, value);
         }
      }
      in.close();
      try {
         pid.waitFor();
      }
      catch (InterruptedException e) {
         throw new IOException(e.getMessage());
      }
   }
   
   // to be used with Runtime.exec(String[] cmdarray, String[] envp) 
   String[] toArray()
   {
     String[] arr = new String[super.size()];
     Enumeration it = super.keys();
     int i = -1;
     while(it.hasMoreElements()) {
        String key = (String)it.nextElement();
        String val = (String)get(key);
        i++;   
        arr[i] = key + "=" + val;
     }     
     return arr;
   }
} 