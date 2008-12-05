package org.apache.hadoop.chukwa.extraction.demux.processor;

import java.io.IOException;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ChukwaOutputCollector implements OutputCollector<ChukwaRecordKey, ChukwaRecord>
{
  private OutputCollector<ChukwaRecordKey, ChukwaRecord> outputCollector = null;
  private Reporter reporter = null;
  private String groupName = null;
  
  public ChukwaOutputCollector(String groupName,OutputCollector<ChukwaRecordKey, ChukwaRecord> outputCollector,Reporter reporter)
  {
    this.reporter = reporter;
    this.outputCollector = outputCollector;
    this.groupName = groupName;
  }

  @Override
  public void collect(ChukwaRecordKey key, ChukwaRecord value)
      throws IOException
  {
    this.outputCollector.collect(key, value);
    reporter.incrCounter(groupName, "total records", 1);
    reporter.incrCounter(groupName,  key.getReduceType() +" records" , 1);
  }


}
