package org.apache.hadoop.chukwa.inputtools.plugin.pbsnode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.hadoop.chukwa.inputtools.plugin.IPlugin;
import org.json.JSONException;
import org.json.JSONObject;

public class PbsNodePlugin extends ExecPlugin
{
	private static Log log = LogFactory.getLog(PbsNodePlugin.class);
	private String cmde = null;
	private DataConfig dataConfig = null;
	
	public PbsNodePlugin()
	{
		dataConfig = new DataConfig();
		cmde = dataConfig.get("chukwa.inputtools.plugin.pbsNode.cmde");
	}
	
	@Override
	public String getCmde()
	{
		return cmde;
	}

	public static void main(String[] args) throws JSONException
	{
		IPlugin plugin = new PbsNodePlugin();
		JSONObject result = plugin.execute();
		System.out.print("Result: " + result);	
		
		if (result.getInt("status") < 0)
		{
			System.out.println("Error");
			log.warn("[ChukwaError]:"+ PbsNodePlugin.class + ", " + result.getString("stderr"));
		}
		else
		{
			log.info(result.get("stdout"));
		}
	}
}
