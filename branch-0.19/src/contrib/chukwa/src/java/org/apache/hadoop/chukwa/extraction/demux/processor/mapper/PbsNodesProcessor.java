package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.database.DatabaseHelper;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class PbsNodesProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(PbsNodesProcessor.class);

	private static final String rawPBSRecordType = "PbsNodes";
	private static final String machinePBSRecordType = "MachinePbsNodes";
	
	private static String regex= null;
	private static Pattern p = null;
	
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;

	public PbsNodesProcessor()
	{
		//TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		regex="([0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}\\:[0-9]{2}:[0-9]{2},[0-9]{3}) (INFO|DEBUG|ERROR|WARN) (.*?): ((.*)\n*)*\\z";
		p = Pattern.compile(regex);
		matcher = p.matcher("-");
	}

	@Override
	protected void parse(String recordEntry, OutputCollector<Text, ChukwaRecord> output,
			Reporter reporter)
	{
		
		log.info("PbsNodeProcessor record: [" + recordEntry + "] type[" + chunk.getDataType() + "]");
		
		

		StringBuilder sb = new StringBuilder(); 	 
		int i = 0;
		String nodeActivityStatus = null;
		int totalFreeNode = 0;
		int totalUsedNode = 0;
		int totalDownNode = 0;
		
		String logLevel = null;
		String className = null;
		String body = null;
		ChukwaRecord record = null;
		
		matcher.reset(recordEntry);
		if (matcher.matches())
		{
			log.info("PbsNodeProcessor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(0).trim());
				
				logLevel = matcher.group(2);
				className = matcher.group(3);
				body = matcher.group(4);
				
				// Raw PBS output
				/*
			    record = new ChukwaRecord();
				buildGenericRecord(record,recordEntry,d.getTime(),rawPBSRecordType);
				
				//TODO create a more specific key structure
				// part of ChukwaArchiveKey + record index if needed
				key.set("" + d.getTime());
				
				record.add(Record.logLevelField, logLevel);
				record.add(Record.classField, className);
				record.add(Record.bodyField, body);
				//Output PbsNode record for all machines (raw information)
				output.collect(key, record);
				log.info("PbsNodeProcessor output 1 record");
				*/
				
				// TODO if we're not saving the PBS' output, we don't need to parse and save it ...
				String[] lines = recordEntry.split("\n");
				while (i < lines.length)
				{
					while ((i < lines.length) && (lines[i].trim().length() > 0))
					{
						sb.append(lines[i].trim()).append("\n");
						i++;
					}

					 if ( (i< lines.length) && (lines[i].trim().length() > 0) )
					 {
					 throw new PbsInvalidEntry(recordEntry);
					 }

					// Empty line
					i++;

					if (sb.length() > 0)
					{
						body =  sb.toString();
						// Process all entries for a machine
						System.out.println("=========>>> Record [" + body+ "]");
						
						record = new ChukwaRecord();
						buildGenericRecord(record,body,d.getTime(),machinePBSRecordType);
						
						// TODO Change the key information
						key.set("" + d.getTime());
						record.add(Record.logLevelField, logLevel);
						record.add(Record.classField, className);
						record.add(Record.bodyField, body);
						
						parsePbsRecord(body, record);
						
						//Output PbsNode record for 1 machine
						output.collect(key, record);
						log.info("PbsNodeProcessor output 1 sub-record");
						//compute Node Activity information
						nodeActivityStatus = record.getValue("state");
						if (nodeActivityStatus != null)
						{
							if (nodeActivityStatus.equals("free"))
							{
								totalFreeNode ++;
							}
							else if (nodeActivityStatus.equals("job-exclusive"))
							{
								totalUsedNode ++;
							}
							else
							{
								totalDownNode ++;
							}					
						}
						sb = new StringBuilder();
					}
				}
				
				// End of parsing

				// database (long timestamp,String table,String tag,int tableType,int sqlType)
				DatabaseHelper databaseRecord = new DatabaseHelper("NodeActivity");
				
				// Data
				databaseRecord.add(d.getTime(),"Used",""+totalUsedNode);
				databaseRecord.add(d.getTime(),"Free",""+totalFreeNode);
				databaseRecord.add(d.getTime(),"Down",""+totalDownNode);
				
				//Output NodeActivity info to database
				output.collect(key, databaseRecord.buildChukwaRecord());
				log.info("PbsNodeProcessor output 1 NodeActivity to database");
				
				// INFO if you need to save NodeActivity info to HDFS
				// use the following block

//				// Save Node Activity information
//				String nodeActivity = "NodeActivity:  totalFreeNode=" + totalFreeNode 
//				+ ",totalUsedNode=" + totalUsedNode 
//				+ ",totalDownNode=" + totalDownNode;
//
//				record = new ChukwaRecord();
//				buildGenericRecord(record,nodeActivity,d.getTime(),nodeActivityRecordType);
//				//Used,Free,Down
//				record.add("Used",""+totalUsedNode);
//				record.add("Free",""+totalFreeNode);
//				record.add("Down",""+totalDownNode);
//				//Output NodeActivity info to HDFS
//				output.collect(key, record);
//				log.info("PbsNodeProcessor output 1 NodeActivity to HDFS");
					
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				log.warn("Wrong format in PbsNodesProcessor [" + recordEntry + "]", e);
			}
			catch (IOException e)
			{
				log.warn("Unable to collect output in PbsNodesProcessor [" + recordEntry + "]", e);
				e.printStackTrace();
			}
			catch (PbsInvalidEntry e)
			{
				log.warn("Wrong format in PbsNodesProcessor [" + recordEntry + "]", e);
				e.printStackTrace();
			}
		}

		
	}

	protected static void parsePbsRecord(String recordLine, ChukwaRecord record)
	{
		int i = 0;
		String[] lines = recordLine.split("\n");
		System.out.println("Machine=" + lines[i]);
		i++;
		String[] data = null;
		while (i < lines.length)
		{
			data = extractFields(lines[i]);
			System.out.println("[" + data[0].trim() + "] ==> ["
					+ data[1].trim() + "]");
			
			record.add(data[0].trim(), data[1].trim());
			
			if (data[0].trim().equalsIgnoreCase("status"))
			{
				parseStatusField(data[1].trim(), record);
			}
			i++;
		}
	}

	protected static void parseStatusField(String statusField,
			ChukwaRecord record)
	{
		String[] data = null;
		String[] subFields = statusField.trim().split(",");
		for (String subflied : subFields)
		{
			data = extractFields(subflied);
			record.add(data[0].trim(), data[1].trim());
			System.out.println("[status." + data[0].trim() + "] ==> ["
					+ data[1].trim() + "]");
		}
	}


	static String[] extractFields(String line)
	{
		String[] args = new String[2];
		int index = line.indexOf("=");
		args[0] = line.substring(0,index );
		args[1] = line.substring(index + 1);

		return args;
	}

	public String getDataType()
	{
		return PbsNodesProcessor.rawPBSRecordType;
	}
	
}
