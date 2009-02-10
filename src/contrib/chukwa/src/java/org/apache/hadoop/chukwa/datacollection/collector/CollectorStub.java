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
import org.apache.hadoop.chukwa.datacollection.writer.*;
import org.apache.hadoop.chukwa.util.PidFile;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;

public class CollectorStub {
  
  static int THREADS = 80;
  private static PidFile pFile = null;
  public static Server jettyServer = null;
  public static void main(String[] args) {
	
	pFile=new PidFile("Collector");
	Runtime.getRuntime().addShutdownHook(pFile); 	 	  
    try {
      System.out.println("usage:  CollectorStub [portno] [pretend]");
      System.out.println("note: if no portno defined, " +
      		"defaults to value in chukwa-site.xml");
 
      ChukwaConfiguration conf = new ChukwaConfiguration();
      int portNum = conf.getInt("chukwaCollector.http.port", 9999);
      THREADS = conf.getInt("chukwaCollector.http.threads", 80);
      
      if(args.length != 0)
        portNum = Integer.parseInt(args[0]);
      
        //pick a writer.
      ChukwaWriter w = null;
      if(args.length > 1) {
        if(args[1].equals("pretend"))
          w= new ConsoleWriter(true);
        else if(args[1].equals("pretend-quietly"))
          w = new ConsoleWriter(false);
        else if(args[1].equals("-classname")) {
          if(args.length < 3)
            System.err.println("need to specify a writer class");
          else {
            conf.set("chukwaCollector.writerClass", args[2]);
          }
        }
        else
          System.out.println("WARNING: unknown command line arg "+ args[1]);
      }
      if(w != null) {
        w.init(conf);
        ServletCollector.setWriter(w);
      }
      
        //set up jetty connector
      SelectChannelConnector jettyConnector = new SelectChannelConnector();
      jettyConnector.setLowResourcesConnections(THREADS-10);
      jettyConnector.setLowResourceMaxIdleTime(1500);
      jettyConnector.setPort(portNum);
        //set up jetty server
      jettyServer = new Server(portNum);
      
      jettyServer.setConnectors(new Connector[]{ jettyConnector});
      org.mortbay.thread.BoundedThreadPool pool = 
        new org.mortbay.thread.BoundedThreadPool();
      pool.setMaxThreads(THREADS);
      jettyServer.setThreadPool(pool);
        //and add the servlet to it
      Context root = new Context(jettyServer,"/",Context.SESSIONS);
      root.addServlet(new ServletHolder(new ServletCollector(conf)), "/*");
      jettyServer.start();
      jettyServer.setStopAtShutdown(false);
     
      System.out.println("started http collector on port number " + portNum);

    } catch(Exception e) {
     e.printStackTrace();
      System.exit(0);
    }

  }

}
