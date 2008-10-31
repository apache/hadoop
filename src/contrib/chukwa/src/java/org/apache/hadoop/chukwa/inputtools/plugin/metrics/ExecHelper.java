package org.apache.hadoop.chukwa.inputtools.plugin.metrics;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.hadoop.chukwa.inputtools.plugin.IPlugin;
import org.apache.hadoop.chukwa.util.PidFile;
import org.json.JSONException;
import org.json.JSONObject;

public class ExecHelper extends ExecPlugin {
	private static Log log = LogFactory.getLog(ExecHelper.class);
	private String cmde = null;
    private static PidFile pFile = null;
    private Timer timer = null;
    
	public ExecHelper(String[] cmds) {
		StringBuffer c = new StringBuffer();
		for(String cmd : cmds) {
			c.append(cmd);
			c.append(" ");
		}
		cmde = c.toString();
	}
	
	public String getCmde() {
		return cmde;
	}    
}
