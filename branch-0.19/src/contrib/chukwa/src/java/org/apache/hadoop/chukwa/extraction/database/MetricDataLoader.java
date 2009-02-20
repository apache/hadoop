package org.apache.hadoop.chukwa.extraction.database;

import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

public class MetricDataLoader {
	
	private DatabaseWriter db = null;
	private DataConfig dc = new DataConfig();
	
	public MetricDataLoader() {
		db = new DatabaseWriter();
	}
	public void load(String src) {
		
	}
	public void save() {
		
	}
	public void process() {
		// open hdfs files and process them.
		String dfs = dc.get("chukwa.engine.dsDirectory.rootFolder");
		load(dfs);
		save();
	}
}
