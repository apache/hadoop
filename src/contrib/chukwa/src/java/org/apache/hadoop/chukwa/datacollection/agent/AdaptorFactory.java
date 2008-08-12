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

/**
 * Produces new unconfigured adaptors, given the class name of the appender type
 * 
 */
public class AdaptorFactory {
   
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
      else return null;
    } catch (Exception e){
      System.out.println("Error instantiating new adaptor by class" + e);
      return null;
    }
    
  }
  
}
