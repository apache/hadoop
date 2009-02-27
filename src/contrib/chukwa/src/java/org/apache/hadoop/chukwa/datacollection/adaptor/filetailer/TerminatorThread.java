package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.FileAdaptor;
import org.apache.log4j.Logger;

public class TerminatorThread extends Thread {
	private static Logger log =Logger.getLogger(FileAdaptor.class);

	private FileTailingAdaptor adaptor = null;
	private ChunkReceiver eq = null;
	
	public TerminatorThread(FileTailingAdaptor adaptor, ChunkReceiver eq) {
		this.adaptor = adaptor;
		this.eq = eq;
	}

  public void run() {
    
    long endTime = System.currentTimeMillis() + (10*60*1000); // now + 10 mins
    int count = 0;
    log.info("Terminator thread started." + adaptor.toWatch.getPath());
    try {
      while (adaptor.tailFile(eq)) {
        if (log.isDebugEnabled()) {
          log.debug("Terminator thread:" + adaptor.toWatch.getPath() + " still working");
        }
        long now = System.currentTimeMillis();
        if (now > endTime ) {
          log.warn("TerminatorThread should have been finished by now! count=" + count);
          count ++;
          endTime = System.currentTimeMillis() + (10*60*1000); // now + 10 mins
          if (count >3 ) {
            log.warn("TerminatorThread should have been finished by now, stopping it now! count=" + count);
            break;
          }
        }
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException on Terminator thread:" + adaptor.toWatch.getPath(),e);
    } catch (Throwable e) {
      log.warn("Exception on Terminator thread:" + adaptor.toWatch.getPath(),e);
    }
    
    log.info("Terminator thread finished." + adaptor.toWatch.getPath());
    try {
      adaptor.reader.close();
    } catch (Throwable ex) {
      // do nothing
    }
  }
}
