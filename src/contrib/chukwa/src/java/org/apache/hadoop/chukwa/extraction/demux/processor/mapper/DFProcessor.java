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

public class DFProcessor extends AbstractProcessor
{
	static Logger log = Logger.getLogger(DFProcessor.class);
	private static final String[] columns = {"Filesystem:","1K-blocks:","Used:","Available:","Use%:","MountedOn:"};
	private static final String[] headerCols = {"Filesystem","1K-blocks","Used","Available","Use%","Mounted on"};
	
	private static String regex= null;
	private static Pattern p = null;
	private Matcher matcher = null;
	private SimpleDateFormat sdf = null;
	
	public DFProcessor()
	{
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
			log.info("PbsNodeProcessor Matches");
			
			try
			{
				Date d = sdf.parse( matcher.group(0).trim());
				
//				String logLevel = matcher.group(2);
//				String className = matcher.group(3);
				String body = matcher.group(4);
			
				String[] lines = body.split("\n");
				
				
				String[] outputCols = lines[0].split("[\\s]++");
				
				if (outputCols.length!=6 && 
					outputCols[0].intern() != headerCols[0].intern() &&
					outputCols[1].intern() != headerCols[1].intern() &&
					outputCols[2].intern() != headerCols[2].intern() &&
					outputCols[3].intern() != headerCols[3].intern() &&
					outputCols[4].intern() != headerCols[4].intern() &&
					outputCols[5].intern() != headerCols[5].intern() )
				{
					throw new DFInvalidRecord("Wrong output format (header) [" + recordEntry + "]");
				}
				
				String[] values = null;
				// database (long timestamp,String table,String tag,int tableType,int sqlType)
				DatabaseHelper databaseRecord = new DatabaseHelper("system");
				
				// Data
				//databaseRecord.addKey("Used",""+totalUsedNode);
				
				for (int i=1;i<lines.length;i++)
				{
					values = lines[i].split("[\\s]++");
					databaseRecord.add(d.getTime(),values[0]+"."+columns[1], values[1]);
					databaseRecord.add(d.getTime(),values[0]+"."+columns[2], values[2]);
					databaseRecord.add(d.getTime(),values[0]+"."+columns[3], values[3]);
					databaseRecord.add(d.getTime(),values[0]+"."+columns[4], values[4]);
					databaseRecord.add(d.getTime(),values[0]+"."+columns[5], values[5]);
				}
				//Output DF info to database
				output.collect(key, databaseRecord.buildChukwaRecord());
				log.info("DFProcessor output 1 DF to database");
			}
			catch (ParseException e)
			{
				e.printStackTrace();
				log.warn("Wrong format in DFProcessor [" + recordEntry + "]", e);
			}
			catch (IOException e)
			{
				e.printStackTrace();
				log.warn("Unable to collect output in DFProcessor [" + recordEntry + "]", e);
			}
			catch (DFInvalidRecord e)
			{
				e.printStackTrace();
				log.warn("Wrong format in DFProcessor [" + recordEntry + "]", e);
			}
	}
	}

	public String getDataType()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
