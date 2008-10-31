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

public class Exec extends TimerTask {
	private static Log log = LogFactory.getLog(Exec.class);
	private String cmde = null;
    private static PidFile pFile = null;
    private Timer timer = null;
    private IPlugin plugin = null;
    
	public Exec(String[] cmds) {
		StringBuffer c = new StringBuffer();
		for(String cmd : cmds) {
			c.append(cmd);
			c.append(" ");
		}
		cmde = c.toString();
		plugin = new ExecHelper(cmds);
	}
	public void run() {
		try {
			JSONObject result = plugin.execute();
			if (result.getInt("status") < 0) {
				System.out.println("Error");
				log.warn("[ChukwaError]:"+ Exec.class + ", " + result.getString("stderr"));
				System.exit(-1);
			} else {
				log.info(result.get("stdout"));
			}
		} catch(JSONException e) {
			log.error("Exec output unparsable:"+this.cmde);
		}
	}
	public String getCmde() {
		return cmde;
	}
    
	public static void main(String[] args) {
   	    pFile=new PidFile(System.getProperty("RECORD_TYPE")+"-data-loader");
   	    Runtime.getRuntime().addShutdownHook(pFile);
   	    int period = 60;
   	    try {
			if(System.getProperty("PERIOD")!=null) {
			    period = Integer.parseInt(System.getProperty("PERIOD"));
			}
        } catch(NumberFormatException ex) {
			ex.printStackTrace();
			System.out.println("Usage: java -DPERIOD=nn -DRECORD_TYPE=recordType Exec [cmd]");
			System.out.println("PERIOD should be numeric format of seconds.");        	
			System.exit(0);
        }
   	    Timer timer = new Timer();
		timer.schedule(new Exec(args),0, period*1000);
	}
}
