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

package org.apache.hadoop.chukwa.datacollection.collector.servlet;

import java.io.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.log4j.Logger;

public class ServletCollector extends HttpServlet
{

  static final boolean FANCY_DIAGNOSTICS = true;
	static org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter writer = null;
	 
  private static final long serialVersionUID = 6286162898591407111L;
  Logger log = Logger.getRootLogger();//.getLogger(ServletCollector.class);
	  
  
	public static void setWriter(org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter w) throws IOException
	{
	  writer = w;
	  w.init();
	}
  
	public void init(ServletConfig servletConf) throws ServletException
	{
	  
	  log.info("initing servletCollector");
		if(servletConf == null)	{
			log.fatal("no servlet config");
			return;
		}
		
		try
		{
			// read the application->pipeline settings from a config file in the format:
			// appliation_name: PipelinedWriter1, PipelinedWriter2, Writer
			// use reflection to set up the pipeline after reading in the list of writers from the config file
			
			/*
			String strPipelines = "HadoopLogs:HdfsWriter\nApplication2:SameerWriter:HdfsWriter";
			String[] pipelines = strPipelines.split("\n");
			// split into pipes
			for (String pipe : pipelines){
				String[] tmp = pipe.split(":");
				String app = tmp[0];
				String[] stages = tmp[1].split(",");
			
				//loop through pipes, creating linked list of stages per pipe, one at a time 
				for (String stage : stages){
					Class curr = ClassLoader.loadClass(stage);
				}
			}
			*/
		      //FIXME: seems weird to initialize a static object here
			if (writer == null)
				writer =  new SeqFileWriter();

		} catch (IOException e) {
			throw new ServletException("Problem init-ing servlet", e);
		}		
	}

	protected void accept(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException
	{
	  ServletDiagnostics diagnosticPage = new ServletDiagnostics();
		try {
	    
		  final long currentTime = System.currentTimeMillis();
		  log.debug("new post from " + req.getRemoteHost() + " at " + currentTime);
			java.io.InputStream in = req.getInputStream();
						
			ServletOutputStream l_out = resp.getOutputStream();
			final DataInputStream di = new DataInputStream(in);
			final int numEvents = di.readInt();
		  //	log.info("saw " + numEvents+ " in request");

      if(FANCY_DIAGNOSTICS)
        diagnosticPage.sawPost(req.getRemoteHost(), numEvents, currentTime);
			for (int i = 0; i < numEvents; i++){
				// TODO: pass new data to all registered stream handler methods for this chunk's stream
				// TODO: should really have some dynamic assignment of events to writers

	      ChunkImpl logEvent =  ChunkImpl.read(di);

	      if(FANCY_DIAGNOSTICS)
	        diagnosticPage.sawChunk(logEvent, i);
	      
				// write new data to data sync file
				if(writer != null) {
				  writer.add(logEvent);  //save() blocks until data is written
				  //this is where we ACK this connection
					l_out.print("ok:");
					l_out.print(logEvent.getData().length);
					l_out.print(" bytes ending at offset ");
					l_out.println(logEvent.getSeqID()-1);
				}
				else
					l_out.println("can't write: no writer");	
			}

      if(FANCY_DIAGNOSTICS)
        diagnosticPage.doneWithPost();
	    resp.setStatus(200);
			
		} catch (IOException e) 	{
			log.warn("IO error", e);
			throw new ServletException(e);
		}
	}

	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException
	{
		accept(req,resp);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException
	{
	  PrintStream out = new PrintStream(resp.getOutputStream());
    resp.setStatus(200);
	  out.println("<html><body><h2>Chukwa servlet running</h2>");
	  if(FANCY_DIAGNOSTICS)
	    ServletDiagnostics.printPage(out);
	  out.println("</body></html>");
//		accept(req,resp);
	}

  @Override	
	public String getServletInfo()
	{
		return "Chukwa Servlet Collector";
	}

	@Override
	public void destroy()
	{
	  synchronized(writer)
	  {
	    writer.close();
	  }
	  super.destroy();
	}
}
