package org.apache.hadoop.chukwa.inputtools.plugin.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.hadoop.chukwa.inputtools.plugin.IPlugin;
import org.json.JSONException;
import org.json.JSONObject;

public class Exec extends ExecPlugin
{
	private static Log log = LogFactory.getLog(Exec.class);
	private String cmde = null;
	
	public Exec(String[] cmds)
	{
		StringBuffer c = new StringBuffer();
		for(String cmd : cmds) {
			c.append(cmd);
			c.append(" ");
		}
		cmde = c.toString();
	}
	
	@Override
	public String getCmde()
	{
		return cmde;
	}

	public static void main(String[] args) throws JSONException
	{
		IPlugin plugin = new Exec(args);
		JSONObject result = plugin.execute();		
		if (result.getInt("status") < 0)
		{
			System.out.println("Error");
			log.warn("[ChukwaError]:"+ Exec.class + ", " + result.getString("stderr"));
			System.exit(-1);
		}
		else
		{
			log.info(result.get("stdout"));
		}
		System.exit(0);
	}
}
