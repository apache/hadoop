package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.database.DatabaseHelper;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class HadoopMetricsProcessor extends AbstractProcessor 
{

	static Logger log = Logger.getLogger(HadoopMetricsProcessor.class);
	
	private static String regex= null;
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;

	public HadoopMetricsProcessor()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		regex="([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): (.*)";
		p = Pattern.compile(regex);
		matcher = p.matcher("-");
	}
	
	
  @Override
  protected void parse(String recordEntry,
      OutputCollector<Text, ChukwaRecord> output, Reporter reporter)
  {
	  
	  matcher.reset(recordEntry);
		if (matcher.matches())
		{
			log.info("HadoopMetricsProcessor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(0).trim());
				key.set("" + d.getTime());
				String body = matcher.group(4);
				
				 String[] kvpairs = body.split(" ");
				    
				// database
				DatabaseHelper databaseRecord = new DatabaseHelper("HadoopMetrics");
				
			    for(int i = 1 ; i < kvpairs.length; ++i) 
			    {
			      String kvpair =  kvpairs[i];
			      String[] halves = kvpair.split("=");
			      if(halves[0].equals("chukwa_timestamp"))
			      {
			        key.set(halves[1]);
			      }
			      else
			      {
			    	  databaseRecord.add(d.getTime(), halves[0], halves[1]);
			      }
			    }
			    
			    //Output NodeActivity info to database
				output.collect(key, databaseRecord.buildChukwaRecord());
				log.info("HadoopMetricsProcessor output 1 Hadoop's Metric to database");
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry + "]", e);
			}
			catch (IOException e)
			{
				e.printStackTrace();
				log.warn("Unable to collect output in HadoopMetricsProcessor [" + recordEntry + "]", e);
			}
		}
  }


	public String getDataType()
	{
		return HadoopMetricsProcessor.class.getName();
	}

}
