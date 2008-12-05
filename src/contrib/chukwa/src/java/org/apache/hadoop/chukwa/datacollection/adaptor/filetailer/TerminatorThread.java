package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.io.IOException;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.FileAdaptor;
import org.apache.hadoop.chukwa.inputtools.plugin.metrics.ExecHelper;
import org.apache.log4j.Logger;

public class TerminatorThread extends Thread {
	private static Logger log =Logger.getLogger(FileAdaptor.class);
	private static FileTailingAdaptor adaptor = null;
	private static ChunkReceiver eq = null;
	
	public TerminatorThread(FileTailingAdaptor adaptor, ChunkReceiver eq) {
		this.adaptor = adaptor;
		this.eq = eq;
	}

	public void run() {
   	    log.info("Terminator thread started.");
  	    try {
  	    	while(adaptor.tailFile(eq)) {
  	    		log.info("");
  	    	}
		} catch (InterruptedException e) {
			log.info("Unable to send data to collector for 60 seconds, force shutdown.");
		}
        log.info("Terminator finished.");
        try {
        	adaptor.reader.close();
        } catch (IOException ex) {
        	
        }
	}
}
