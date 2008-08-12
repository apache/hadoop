package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.database.DatabaseHelper;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class YWatchProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(PbsNodesProcessor.class);

	private static final String ywatchType = "YWatch";
	
	private static String regex= null;
	
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;

	public YWatchProcessor()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		regex="([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): (.*)";
		p = Pattern.compile(regex);
		matcher = p.matcher("-");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void parse(String recordEntry, OutputCollector<Text, ChukwaRecord> output,
			Reporter reporter)
	{
		
		log.info("YWatchProcessor record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		
		
		matcher.reset(recordEntry);
		if (matcher.matches())
		{
			log.info("YWatchProcessor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(0).trim());
				key.set("" + d.getTime());
				String body = matcher.group(4);
				
				try
				{
					JSONObject json = new JSONObject(body);
					
					// database
					DatabaseHelper databaseRecord = new DatabaseHelper("switches");//
					String tag = json.getString("poller") + "-" + json.getString("host");
					String metricName = json.getString("metricName");
					
					// Data
					JSONObject jsonData = json.getJSONObject("data").getJSONObject("data");
					
				
					long ts = 0;
					double value = 0;
					String jsonTs = null;
					String jsonValue = null;
					Iterator<String> it = jsonData.keys();
					while(it.hasNext())
					{
						jsonTs = it.next();
						jsonValue = jsonData.getString(jsonTs);
						try
						{
							value = Double.parseDouble(jsonValue);
							ts = Long.parseLong(jsonTs);
							databaseRecord.add(ts,metricName,value);
						}
						catch(NumberFormatException e)
						{
							if (!jsonValue.equalsIgnoreCase("N/A"))
							{
								throw new YwatchInvalidEntry("YWatchProcessor invalid entry [" + body + "]");
							}
						}
					}
					
					output.collect(key, databaseRecord.buildChukwaRecord());
					log.info("YWatchProcessor output 1 metric to database");
					
				} 
				catch (IOException e)
				{
					log.warn("Unable to collect output in YWatchProcessor [" + recordEntry + "]", e);
					e.printStackTrace();
				}
				catch (JSONException e)
				{
					e.printStackTrace();
					log.warn("Wrong format in YWatchProcessor [" + recordEntry + "]", e);
				}
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	public String getDataType()
	{
		return YWatchProcessor.ywatchType;
	}
}
