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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.*;
import org.apache.log4j.Logger;

public class ServletCollector extends HttpServlet
{

  static final boolean FANCY_DIAGNOSTICS = false;
	static org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter writer = null;
	 
  private static final long serialVersionUID = 6286162898591407111L;
  Logger log = Logger.getRootLogger();//.getLogger(ServletCollector.class);
  
	public static void setWriter(org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter w) throws WriterException
	{
	  writer = w;
	}
	static long statTime = 0L;
	static int numberHTTPConnection = 0;
	static int numberchunks = 0;
	
	Configuration conf;
  
  public ServletCollector(Configuration c) {
    conf =c;
  }

	
	public void init(ServletConfig servletConf) throws ServletException
	{
	  
	  log.info("initing servletCollector");
		if(servletConf == null)	{
			log.fatal("no servlet config");
			return;
		}
		
		Timer statTimer = new Timer();
		statTimer.schedule(new TimerTask()
		{
			public void run() 
			{
				log.info("stats:ServletCollector,numberHTTPConnection:" + numberHTTPConnection
						 + ",numberchunks:"+numberchunks);
				statTime = System.currentTimeMillis();
				numberHTTPConnection = 0;
				numberchunks = 0;
			}
		}, (1000), (60*1000));
		
		if(writer != null) {
		  log.info("writer set up statically, no need for Collector.init() to do it");
		  return;
		}
		
		try {
	   String writerClassName = conf.get("chukwaCollector.writerClass", 
	          SeqFileWriter.class.getCanonicalName());
	    Class<?> writerClass = Class.forName(writerClassName);
	    if(writerClass != null &&ChukwaWriter.class.isAssignableFrom(writerClass))
	        writer = (ChukwaWriter) writerClass.newInstance();
		} catch(Exception e) {
		  log.warn("failed to use user-chosen writer class, defaulting to SeqFileWriter", e);
		}
      
    //We default to here if the pipeline construction failed or didn't happen.
    try{ 
      if(writer == null)
        writer =  new SeqFileWriter();//default to SeqFileWriter
      writer.init(conf);
		} catch (WriterException e) {
			throw new ServletException("Problem init-ing servlet", e);
		}		
	}

	protected void accept(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException
	{
		numberHTTPConnection ++;
		ServletDiagnostics diagnosticPage = new ServletDiagnostics();
		final long currentTime = System.currentTimeMillis();
		try {

			log.debug("new post from " + req.getRemoteHost() + " at " + currentTime);
			java.io.InputStream in = req.getInputStream();

			ServletOutputStream l_out = resp.getOutputStream();
			final DataInputStream di = new DataInputStream(in);
			final int numEvents = di.readInt();
			//	log.info("saw " + numEvents+ " in request");

			if(FANCY_DIAGNOSTICS)
			{ diagnosticPage.sawPost(req.getRemoteHost(), numEvents, currentTime); }

			List<Chunk> events = new LinkedList<Chunk>();
			ChunkImpl logEvent = null;
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < numEvents; i++)
			{
				// TODO: pass new data to all registered stream handler 
			  //       methods for this chunk's stream
				// TODO: should really have some dynamic assignment of events to writers

				logEvent =  ChunkImpl.read(di);
				sb.append("ok:");
				sb.append(logEvent.getData().length);
				sb.append(" bytes ending at offset ");
				sb.append(logEvent.getSeqID()-1).append("\n");

				events.add(logEvent);

				if(FANCY_DIAGNOSTICS)
				{ diagnosticPage.sawChunk(logEvent, i); }
			}

			// write new data to data sync file
			if(writer != null) 
			{
				writer.add(events);
				numberchunks += events.size();
				//this is where we ACK this connection
				l_out.print(sb.toString());
			}
			else
			{
				l_out.println("can't write: no writer");
			}


			if(FANCY_DIAGNOSTICS)
			{ diagnosticPage.doneWithPost(); }

			resp.setStatus(200);

		} 
		catch(Throwable e) 
		{
			log.warn("Exception talking to " +req.getRemoteHost() + " at " + currentTime , e);
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
		
	  String pingAtt = req.getParameter("ping");
	  if (pingAtt!=null)
	  {
		  out.println("Date:" + ServletCollector.statTime);
		  out.println("Now:" + System.currentTimeMillis());
		  out.println("numberHTTPConnection:" + ServletCollector.numberHTTPConnection);
		  out.println("numberchunks:" + ServletCollector.numberchunks);
	  }
	  else
	  {
		  out.println("<html><body><h2>Chukwa servlet running</h2>");
		  if(FANCY_DIAGNOSTICS)
		    ServletDiagnostics.printPage(out);
		  out.println("</body></html>");
	  }
    
	  
	}

    @Override	
	public String getServletInfo()
	{
		return "Chukwa Servlet Collector";
	}

	@Override
	public void destroy()
	{
	  try
	{
		writer.close();
	} catch (WriterException e)
	{
		log.warn("Exception during close", e);
		e.printStackTrace();
	}
	  super.destroy();
	}
}
