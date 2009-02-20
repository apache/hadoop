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

package org.apache.hadoop.chukwa.inputtools.log4j;

import org.apache.log4j.*;
import org.apache.log4j.spi.LoggingEvent;

public class OneLineLogLayout extends PatternLayout {
  
  char SEP = ' ';
  public String format(LoggingEvent evt)
  {
    
    String initial_s = super.format(evt);
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < initial_s.length() -1 ; ++i)
    {
      char c = initial_s.charAt(i);
      if(c == '\n')
        sb.append(SEP);
      else
        sb.append(c);
    }
    sb.append(SEP);
    String[] s = evt.getThrowableStrRep();
    if (s != null) {
      int len = s.length;
      for(int i = 0; i < len; i++) {
        sb.append(s[i]);
        sb.append(SEP);
        }
    }
    
    sb.append('\n');
    return sb.toString();
  }
  
  public boolean ignoresThrowable()
  {
    return false;
  }
  
  public static void main(String[] args)
  {
    System.setProperty("line.separator", " ");
    Logger l = Logger.getRootLogger();
    l.removeAllAppenders();
    Appender appender = new ConsoleAppender(new OneLineLogLayout());
    appender.setName("console");
    l.addAppender(appender);
    l.warn("testing", new java.io.IOException("just kidding!"));
    
    
  }

}
