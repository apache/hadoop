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

package org.apache.hadoop.chukwa.inputtools.plugin.nodeactivity;

import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.hadoop.chukwa.inputtools.plugin.IPlugin;
import org.json.JSONObject;

public class NodeActivityPlugin extends ExecPlugin
{
	private String cmde = null;
	private DataConfig dataConfig = null;
	
	public NodeActivityPlugin()
	{
		dataConfig = new DataConfig();
		cmde = dataConfig.get("mdl.plugin.NodeActivityPlugin.cmde");
	}
	
	@Override
	public String getCmde()
	{
		return cmde;
	}
	
	@Override
	public JSONObject postProcess(JSONObject execResult)
	{
		try
		{
			if (execResult.getInt("status") < 0)
			{
				return execResult;
			}
			
			String res = execResult.getString("stdout");
			
			String[] tab = res.split("\n");
			int totalFreeNode = 0;
			int totalUsedNode = 0;
			int totalDownNode = 0;
			
			for(int i=0;i<tab.length;i++)
			{
				if (tab[i].indexOf("state =") <0)
				{
					tab[i] = null;
					continue;
				}
	
				String[] line = tab[i].split("state =");
				tab[i] = null;
				
				if (line[1].trim().equals("free"))
				{
					totalFreeNode ++;
				}
				else if (line[1].trim().equals("job-exclusive"))
				{
					totalUsedNode ++;
				}
				else
				{
					totalDownNode ++;
				}
			}
			

			execResult.put("totalFreeNode", totalFreeNode);
			execResult.put("totalUsedNode", totalUsedNode);
			execResult.put("totalDownNode", totalDownNode);
			execResult.put("source", "NodeActivity");
			
			execResult.put("status", 100);	
			
		} catch (Throwable e)
		{
			try
			{
				execResult.put("source", "NodeActivity");
				execResult.put("status", -100);	
				execResult.put("errorLog",e.getMessage());
			}
			catch(Exception e1) { e1.printStackTrace();}
			e.printStackTrace();
			
		}
		
		return execResult;
	}

	public static void main(String[] args)
	{
		IPlugin plugin = new NodeActivityPlugin();
		JSONObject result = plugin.execute();
		System.out.print("Result: " + result);
		
		
		
		
	}



}
