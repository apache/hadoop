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

package org.apache.hadoop.chukwa.datacollection.agent;

import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.log4j.Logger;

/**
 * Produces new unconfigured adaptors, given the class name of the appender type
 * 
 */
public class AdaptorFactory {
   
  static Logger log = Logger.getLogger(ChukwaAgent.class);
    /**
     * Instantiate an adaptor that can be added by the {@link ChukwaAgent}
     * @param className the name of the {@link Adaptor} class to instantiate
     * @return an Adaptor of the specified type
     */
    static Adaptor createAdaptor(String className){
    Object obj = null;
    try{
      //the following reflection business for type checking is probably unnecessary
      //since it will just throw a ClassCastException on error anyway.
      obj = Class.forName(className).newInstance();
      if (Adaptor.class.isInstance(obj)){
        return (Adaptor) obj;
      } 
      else        
        return null;
    } catch (Exception e1){
      log.warn("Error instantiating new adaptor by class name, " +
      		"attempting again, but with default chukwa package prepended, i.e. " +
      		"org.apache.hadoop.chukwa.datacollection.adaptor." + className + ". " 
      		+ e1);
      try{
        //if failed, try adding default class prefix
        Object obj2 = Class.forName(
            "org.apache.hadoop.chukwa.datacollection.adaptor." +
            className).newInstance();
        if (Adaptor.class.isInstance(obj2)){
          log.debug("Succeeded in finding class by adding default chukwa " +
              "namespace prefix to class name profided");
          return (Adaptor) obj2;
        } 
        else        
          return null;
      } catch (Exception e2) {
        System.out.println("Also error instantiating new adaptor by classname" +
        		"with prefix added" + e2);
        return null;
      }
    }
  }
  
}
