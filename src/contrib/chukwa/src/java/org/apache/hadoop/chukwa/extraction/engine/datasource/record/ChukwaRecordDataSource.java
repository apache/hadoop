package org.apache.hadoop.chukwa.extraction.engine.datasource.record;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaSearchResult;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.extraction.engine.SearchResult;
import org.apache.hadoop.chukwa.extraction.engine.Token;
import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSource;
import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSourceException;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

public class ChukwaRecordDataSource implements DataSource
{
	//TODO need some cleanup after 1st production
	// First implementation to get it working with the new directory structure
	
	static Logger log = Logger.getLogger(ChukwaRecordDataSource.class);
	
	private static final int dayFolder = 100;
	private static final int hourFolder = 200;
	private static final int rawFolder = 300;
	
	static final String[] raws = {"0","5","10","15","20","25","30","35","40","45","50","55"};
	
	private static FileSystem fs = null;
	private static ChukwaConfiguration conf = null;
	
	private static String rootDsFolder = null;
	private static DataConfig dataConfig = null;
	
	static
	{
		dataConfig = new DataConfig();
		rootDsFolder = dataConfig.get("chukwa.engine.dsDirectory.rootFolder");
		conf = new ChukwaConfiguration();
		try
		{
			fs = FileSystem.get(conf);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean isThreadSafe()
	{
		return true;
	}


	@Override
	public SearchResult search(SearchResult result, String cluster,
			String dataSource, long t0, long t1, String filter,Token token)
			throws DataSourceException
	{
		String filePath = rootDsFolder + "/" + cluster + "/";

		log.debug("filePath [" + filePath + "]");	
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(t0);
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
		int maxCount = 200;
		
		List<Record> records = new ArrayList<Record>();

		ChukwaDSInternalResult res =  new ChukwaDSInternalResult();
		
		if (token != null)
		{
			// token.key = day + "|" + hour + "|" + raw + "|" + spill + "|" + res.currentTs + "|"+ res.position + "|"+ res.fileName;
			try
			{
				String[] vars = token.key.split("\\|");
				res.day = vars[0];
				res.hour = Integer.parseInt(vars[1]);
				res.rawIndex = Integer.parseInt(vars[2]);
				res.spill = Integer.parseInt(vars[3]);
				res.currentTs = Long.parseLong(vars[4]);
				res.position = Long.parseLong(vars[5]);
				res.fileName = vars[5];
				log.info("Token is not null! :" + token.key);
			}
			catch(Exception e)
			{
				log.error("Incalid Key: [" + token.key + "] exception: ", e);
			}
		}
		else
		{
			log.debug("Token is  null!" );
		}
		
		try
		{
			do
			{
				log.debug("start Date [" + calendar.getTime() + "]");
				String workingDay = sdf.format(calendar.getTime());
				int workingHour = calendar.get(Calendar.HOUR_OF_DAY);
				int startRawIndex = 0;
				if (token !=null)
				{
					workingDay = res.day ;
					workingHour = res.hour;
					startRawIndex = res.rawIndex;
				}
				else
				{
					token = new Token();
				}
				
				log.debug("workingDay " + workingDay);
				log.debug("workingHour " + workingHour);
				
				if (exist(dayFolder,filePath,dataSource,workingDay,null,null))
				{
					// Extract Data for Day
					if (containsRotateFlag(dayFolder,filePath,dataSource,workingDay,null))
					{
						// read data from day
						// SystemMetrics/20080922/SystemMetrics_20080922.1.evt
						log.debug("fs.exists(workingDayRotatePath) ");
						extractRecords(res,ChukwaRecordDataSource.dayFolder,filePath,dataSource,workingDay, null, -1,
								 token, records, maxCount, t0, t1, filter);
						maxCount = maxCount - records.size();
						if ( (maxCount <= 0) || (res.currentTs > t1))
						 { break; }
						
					} // End process Day File
					else // check for hours
					{
						log.debug("check for hours");
						for (int hour = 0; hour<24;hour ++)
						{
							if ( workingDay == res.day && hour<workingHour)
							{
								continue;
							}
							log.debug(" Hour?  -->" + filePath + dataSource + "/"+ workingDay+ "/" + hour);
							if (exist(dayFolder,filePath,dataSource,workingDay,""+hour,null))
							{
								if (containsRotateFlag(dayFolder,filePath,dataSource,workingDay,""+hour))
								{
									// read data from Hour
									// SystemMetrics/20080922/12/SystemMetrics_20080922_12.1.evt
									extractRecords(res,ChukwaRecordDataSource.hourFolder,filePath,dataSource,workingDay, ""+hour, -1,
											 token, records, maxCount, t0, t1, filter);
								}
								else // check for raw
								{
									log.debug("Working on Raw");
									
									for(int rawIndex=startRawIndex;rawIndex<12;rawIndex++)
									{
										// read data from Raw
										// SystemMetrics/20080922/0/25/SystemMetrics_20080922_0_25.1.evt
										if (exist(dayFolder,filePath,dataSource,workingDay,""+hour,raws[rawIndex]))
										{
											extractRecords(res,ChukwaRecordDataSource.rawFolder,filePath,dataSource,workingDay, ""+hour, rawIndex,
													 token, records, maxCount, t0, t1, filter);
											maxCount = maxCount - records.size();
											if ( (maxCount <= 0) || (res.currentTs > t1))
											 { break; }
										}
										else
										{
											log.debug("<<<<<<<<<Working on Raw Not exist--> "
													+ filePath + dataSource + "/" + workingDay+ "/" + workingHour + "/" + raws[rawIndex] );
										}
										res.spill = 1;
									}
								}
							} // End if (fs.exists(new Path(filePath + workingDay+ "/" + hour)))
							 
							maxCount = maxCount - records.size();
							if ( (maxCount <= 0) || (res.currentTs > t1))
							 { break; }

						} // End process all Hourly/raw files
					}
				}	
				
				maxCount = maxCount - records.size();
				if ( (maxCount <= 0) || (res.currentTs > t1))
				 { break; }
			
				// move to the next day
				calendar.add(Calendar.DAY_OF_MONTH, +1);
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				
			}
			while (calendar.getTimeInMillis() < t1); 
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new DataSourceException(e);
		}
	
		TreeMap<Long, List<Record>> recordsInResult = result.getRecords();
		for (Record record : records)
		{
			long timestamp = record.getTime();
			if (recordsInResult.containsKey(timestamp))
			   {
				recordsInResult.get(timestamp).add(record);
			   }
			   else
			   {
				   List<Record> list = new LinkedList<Record>();
				   list.add(record);
				   recordsInResult.put(timestamp, list);
			   }   
		}
		result.setToken(token);
		return result;

	}

	public void extractRecords(ChukwaDSInternalResult res,int directoryType,String rootFolder,String dataSource,String day,String hour,int rawIndex,
								Token token,List<Record> records,int maxRows,long t0,long t1,String filter) throws Exception
	{
		// for each spill file
		// extract records
		int spill = res.spill;
		
		boolean workdone = false;
		do
		{
			String fileName = buildFileName(directoryType,rootFolder,dataSource,spill,day,hour,rawIndex);
			log.debug("extractRecords : " + fileName);
			
			if (fs.exists(new Path(fileName)))
			{
				 readData(res,token,fileName,maxRows,t0,t1,filter);
				 res.spill = spill;
				 List<Record> localRecords = res.records;
				log.debug("localRecords size : " + localRecords.size());
				maxRows = maxRows - localRecords.size();
				if (maxRows <= 0)
				{
					workdone = true;
				}
				records.addAll(localRecords);
				log.debug("AFTER fileName  [" +fileName + "] count=" + localRecords.size() + " maxCount=" + maxRows);
				spill ++;
			}
			else
			{
				// no more spill
				workdone = true;
			}
		}
		while(!workdone);
		token.key = day + "|" + hour + "|" + rawIndex + "|" + spill + "|" + res.currentTs + "|"+ res.position + "|" + res.fileName;
	}
	
	
	public  void readData(ChukwaDSInternalResult res,Token token,String fileName,int maxRows,long t0, long t1,String filter) throws
			Exception
	{
		List<Record> records = new LinkedList<Record>();
		res.records = records;	
		SequenceFile.Reader r= null;
		if (filter != null)
			{ filter = filter.toLowerCase();}
		
		try
		{
			
			if (!fs.exists(new Path(fileName)))
			{
				log.debug("fileName not there!");
				return;
			}
			log.debug("Parser Open [" +fileName + "]");
			
			long timestamp = 0;
			int listSize = 0;
			ChukwaRecordKey key = new ChukwaRecordKey();
		    ChukwaRecord record = new ChukwaRecord();
		    
			r= new SequenceFile.Reader(fs, new Path(fileName), conf);
			
			log.debug("readData Open2 [" +fileName + "]");
			if ( (fileName.equals(res.fileName)) && (res.position != -1))
			{
				r.seek(res.position);
			}
			res.fileName = 	fileName;
			
			while(r.next(key, record))
			{	
				if (record != null)
				{
					res.position = r.getPosition();
					
					timestamp = record.getTime();
					res.currentTs = timestamp;
					log.debug("\nSearch for startDate: " + new Date(t0) + " is :" + new Date(timestamp));
					
					if (timestamp < t0) 
					{
						 //log.debug("Line not in range. Skipping: " +record);
						 continue;
					} 
					else if (timestamp < t1) 
					{
						log.debug("In Range: " + record.toString());
						boolean valid = false;
						
						 if ( (filter == null || filter.equals("") ))
						 {
							 valid = true;
						 }
						 else if ( isValid(record,filter))
						 {
							 valid = true;
						 }
						 
						 if (valid)
						 {
							records.add(record);
							record = new ChukwaRecord();
							listSize = records.size();
							if (listSize >= maxRows)
							{
								// maxRow so stop here
								//Update token
								token.key = key.getKey();
								token.hasMore = true;
								break;
							}
						 }
						else 
						{
							log.debug("In Range ==================>>>>>>>>> OUT Regex: " + record);
						}
					}
					else
					{
						 log.debug("Line out of range. Stopping now: " +record);
						 // Update Token
						 token.key = key.getKey();
						 token.hasMore = false;
						 break;
					}
				}
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				r.close();
			}
			catch(Exception e){}
		}
	}
	
	public boolean containsRotateFlag(int directoryType,String rootFolder,String dataSource,String workingDay,String workingHour) throws Exception
	{
		boolean contains = false;
		switch(directoryType)
		{
			case	ChukwaRecordDataSource.dayFolder:
				//	SystemMetrics/20080922/rotateDone	
				contains = fs.exists(new Path( rootFolder + dataSource + "/" + workingDay+"/rotateDone"));
			break;
			
			case	ChukwaRecordDataSource.hourFolder:
				// SystemMetrics/20080922/12/rotateDone
				contains =  fs.exists(new Path( rootFolder + dataSource + "/" + workingDay+ "/" + workingHour +"/rotateDone"));
			break;
			
		}
		return contains;
	}
	
	public boolean exist(int directoryType,String rootFolder,String dataSource,String workingDay,String workingHour,String raw) throws Exception
	{
		boolean contains = false;
		switch(directoryType)
		{
			case	ChukwaRecordDataSource.dayFolder:
				//	SystemMetrics/20080922/rotateDone	
				contains = fs.exists(new Path( rootFolder + dataSource + "/" + workingDay));
			break;
			
			case	ChukwaRecordDataSource.hourFolder:
				// SystemMetrics/20080922/12/rotateDone
				contains =  fs.exists(new Path( rootFolder + dataSource + "/" + workingDay+ "/" + workingHour ));
			break;
			case	ChukwaRecordDataSource.rawFolder:
				// SystemMetrics/20080922/12/rotateDone
				contains =  fs.exists(new Path( rootFolder + dataSource + "/" + workingDay+ "/" + workingHour + "/" + raw));
			break;
			
		}
		return contains;
	}
	
	
	protected boolean isValid(ChukwaRecord record, String filter)
	{
		String[] fields = record.getFields();
		for(String field: fields)
		{
			if ( record.getValue(field).toLowerCase().indexOf(filter) >= 0)
			{
				return true;
			}
		}
		return false;
	}
	
	public String buildFileName(int directoryType,String rootFolder,String dataSource,int spill,String day,String hour,int rawIndex)
	{
		String fileName = null;
		// TODO use StringBuilder
		// TODO revisit the way we're building fileName
		
		switch(directoryType)
		{
			case	ChukwaRecordDataSource.dayFolder:
				//	SystemMetrics/20080922/SystemMetrics_20080922.1.evt	
				fileName = rootFolder + "/" + dataSource + "/" + day + "/" 
					+ dataSource + "_" +  day + "." + spill + ".evt";
			break;
			
			case	ChukwaRecordDataSource.hourFolder:
				// SystemMetrics/20080922/12/SystemMetrics_20080922_12.1.evt
				fileName = rootFolder + "/" + dataSource + "/" + day + "/" + hour + "/" 
					+ dataSource + "_" +  day + "_" + hour + "." + spill + ".evt";
			break;
			
			case	ChukwaRecordDataSource.rawFolder:
				// SystemMetrics/20080922/0/25/SystemMetrics_20080922_0_25.1.evt	
				fileName = rootFolder + "/" + dataSource + "/" + day + "/" + hour + "/"  + raws[rawIndex] + "/"
					+ dataSource + "_" +  day + "_" + hour + "_" + raws[rawIndex] + "." + spill + ".evt";
			break;
		}
		log.debug("buildFileName :" + fileName);
		return fileName;
	}
	
	public static void main(String[] args) throws DataSourceException
	{
		ChukwaRecordDataSource ds = new ChukwaRecordDataSource();
		SearchResult result = new ChukwaSearchResult();
		result.setRecords( new TreeMap<Long,List<Record>>());
		String cluster = args[0];
		String dataSource = args[1];
		long t0 = Long.parseLong(args[2]);
		long t1 = Long.parseLong(args[3]);
		String filter = null;
		Token token = null;
		
		if (args.length >= 5 && !args[4].equalsIgnoreCase("null"))
		{
			filter = args[4];
		}
		if (args.length == 6)
		{
			token = new Token();
			token.key = args[5];
			System.out.println("token :" + token.key);
		}
		
		System.out.println("cluster :" + cluster);
		System.out.println("dataSource :" + dataSource);
		System.out.println("t0 :" + t0);
		System.out.println("t1 :" + t1);
		System.out.println("filter :" +filter );
		
		
		ds.search(result, cluster, dataSource, t0, t1, filter,token);
		TreeMap<Long, List<Record>> records = result.getRecords();
		Iterator<Long> it = records.keySet().iterator();
		
		while(it.hasNext())
		{
			long ts = it.next();
			System.out.println("\n\nTimestamp: " + new Date(ts));
			List<Record> list = records.get(ts);
			for (int i=0;i<list.size();i++)
			{
				System.out.println(list.get(i));
			}
		}
		
		if (result.getToken() != null)
		 { System.out.println("Key -->" + result.getToken().key);}
	}
}
