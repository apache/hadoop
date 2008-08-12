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

package org.apache.hadoop.chukwa.datacollection.collector;

import org.mortbay.jetty.*;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.*;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.writer.ConsoleWriter;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;

public class CollectorStub {
  
  
  public static void main(String[] args)
  {
    
    try {
      System.out.println("usage:  CollectorStub [portno] [pretend]");
      System.out.println("note: if no portno defined, defaults to value in chukwa-site.xml");
 
      ChukwaConfiguration conf = new ChukwaConfiguration();
      int portNum = conf.getInt("chukwaCollector.http.port", 9999);

      if(args.length != 0)
        portNum = Integer.parseInt(args[0]);
      if(args.length > 1) {
        if(args[1].equals("pretend"))
          ServletCollector.setWriter(new ConsoleWriter(true));
        else if(args[1].equals("pretend-quietly"))
          ServletCollector.setWriter(new ConsoleWriter(false));
        else
          System.out.println("WARNING: don't know what to do with command line arg "+ args[1]);
      }
      
      SelectChannelConnector jettyConnector = new SelectChannelConnector();
      jettyConnector.setLowResourcesConnections(20);
      jettyConnector.setLowResourceMaxIdleTime(1000);
      jettyConnector.setPort(portNum);
      Server server = new Server(portNum);
      server.setConnectors(new Connector[]{ jettyConnector});
      org.mortbay.thread.BoundedThreadPool pool = new  org.mortbay.thread.BoundedThreadPool();
      pool.setMaxThreads(30);
      server.setThreadPool(pool);
      Context root = new Context(server,"/",Context.SESSIONS);
      root.addServlet(new ServletHolder(new ServletCollector()), "/*");
      server.start();
      server.setStopAtShutdown(false);
     
      System.out.println("started http collector on port number " + portNum);

    }
    catch(Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }

  }

}
