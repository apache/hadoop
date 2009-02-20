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

package org.apache.hadoop.chukwa.inputtools.plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.json.JSONObject;

public abstract class ExecPlugin implements IPlugin
{
	public final int statusOK = 100;
	public final int statusKO = -100;
	
	Process process;
	
	public ExecPlugin()
	{
		
	}
	
	public void stop() {
	  process.destroy();
	}
	
	public int waitFor() throws InterruptedException {
	  return process.waitFor();
	}
	
	public abstract String getCmde();
	
	public JSONObject postProcess(JSONObject execResult)
	{
		return execResult;
	}
	
	public JSONObject execute()
	{
		JSONObject result = new JSONObject();
		try
		{
			result.put("timestamp", System.currentTimeMillis());
			
			Runtime runtime = Runtime.getRuntime();
			process = runtime.exec(getCmde());
//			ProcessBuilder builder = new ProcessBuilder(cmde);
//			Process process = builder.start();
			

			
			
			OutputReader stdOut = new OutputReader(process,Output.stdOut);
			stdOut.start();
			OutputReader stdErr = new OutputReader(process,Output.stdErr);
			stdErr.start();
		    int exitValue =process.waitFor();
		    stdOut.join();
		    stdErr.join();
		    result.put("exitValue", exitValue);
		    result.put("stdout", stdOut.output.toString());
		    result.put("stderr", stdErr.output.toString());
		    result.put("status", statusOK);
		}
		catch (Throwable e)
		{
			try 
			{
				result.put("status", statusKO);
				result.put("errorLog", e.getMessage());
			}
			catch(Exception e1) { e1.printStackTrace();}
			e.printStackTrace();
		}

		return postProcess(result);
	}
}


enum Output{stdOut,stdErr};

class OutputReader extends Thread
{
	private Process process = null;
	private Output outputType = null;
	public StringBuilder output = new StringBuilder();
	public boolean isOk = true;


	public OutputReader(Process process,Output outputType)
	{
		this.process = process;
		this.outputType = outputType;
	}
	public void run()
	{
	   try
		{
		    String line = null;
		    InputStream is = null;
		    switch(this.outputType)
		    {
		    case stdOut:
		    	is = process.getInputStream();
		    	break;
		    case stdErr:
		    	is = process.getErrorStream();
		    	break;
		    	
		    }
		   
		    InputStreamReader isr = new InputStreamReader(is);
		    BufferedReader br = new BufferedReader(isr);
		    while ((line = br.readLine()) != null) 
		    {
		    	 //System.out.println("========>>>>>>>["+line+"]");	
		    	 output.append(line).append("\n");
		    }
		}
		catch (IOException e)
		{
			isOk = false;
			e.printStackTrace();
		}
		catch (Throwable e)
		{
			isOk = false;
			e.printStackTrace();
		}
	}
}
