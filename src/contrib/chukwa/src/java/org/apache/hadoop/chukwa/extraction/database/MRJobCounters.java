package org.apache.hadoop.chukwa.extraction.database;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.io.SequenceFile.Reader;

public class MRJobCounters implements DBPlugin
{
	private static Log log = LogFactory.getLog(MRJobCounters.class);
	
	static final String[] fields = 
		{"FILE_SYSTEMS_HDFS_BYTES_READ","FILE_SYSTEMS_HDFS_BYTES_WRITTEN",
		"FILE_SYSTEMS_LOCAL_BYTES_READ","FILE_SYSTEMS_LOCAL_BYTES_WRITTEN","HodId",
		"JOB_COUNTERS__DATA-LOCAL_MAP_TASKS","JOB_COUNTERS__LAUNCHED_MAP_TASKS",
		"JOB_COUNTERS__LAUNCHED_REDUCE_TASKS","JOB_COUNTERS__RACK-LOCAL_MAP_TASKS",
		"JobId","MAP-REDUCE_FRAMEWORK_COMBINE_INPUT_RECORDS","MAP-REDUCE_FRAMEWORK_COMBINE_OUTPUT_RECORDS",
		"MAP-REDUCE_FRAMEWORK_MAP_INPUT_BYTES","MAP-REDUCE_FRAMEWORK_MAP_INPUT_RECORDS",
		"MAP-REDUCE_FRAMEWORK_MAP_OUTPUT_BYTES","MAP-REDUCE_FRAMEWORK_MAP_OUTPUT_RECORDS",
		"MAP-REDUCE_FRAMEWORK_MAP_OUTPUT_BYTES","MAP-REDUCE_FRAMEWORK_MAP_OUTPUT_RECORDS",
		"MAP-REDUCE_FRAMEWORK_REDUCE_INPUT_GROUPS","MAP-REDUCE_FRAMEWORK_REDUCE_INPUT_RECORDS",
		"MAP-REDUCE_FRAMEWORK_REDUCE_OUTPUT_RECORDS"};
//	[FILE_SYSTEMS_HDFS_BYTES_READ] :801280331655
//	[FILE_SYSTEMS_HDFS_BYTES_WRITTEN] :44142889
//	[FILE_SYSTEMS_LOCAL_BYTES_READ] :1735570776310
//	[FILE_SYSTEMS_LOCAL_BYTES_WRITTEN] :2610893176016
//	[HodId] :0.0
//	[JOB_COUNTERS__DATA-LOCAL_MAP_TASKS] :5545
//	[JOB_COUNTERS__LAUNCHED_MAP_TASKS] :5912
//	[JOB_COUNTERS__LAUNCHED_REDUCE_TASKS] :739
//	[JOB_COUNTERS__RACK-LOCAL_MAP_TASKS] :346
//	[JobId] :2.008042104030008E15
//	[MAP-REDUCE_FRAMEWORK_COMBINE_INPUT_RECORDS] :0
//	[MAP-REDUCE_FRAMEWORK_COMBINE_OUTPUT_RECORDS] :0
//	[MAP-REDUCE_FRAMEWORK_MAP_INPUT_BYTES] :801273929542
//	[MAP-REDUCE_FRAMEWORK_MAP_INPUT_RECORDS] :9406887059
//	[MAP-REDUCE_FRAMEWORK_MAP_OUTPUT_BYTES] :784109666437
//	[MAP-REDUCE_FRAMEWORK_MAP_OUTPUT_RECORDS] :9406887059
//	[MAP-REDUCE_FRAMEWORK_REDUCE_INPUT_GROUPS] :477623
//	[MAP-REDUCE_FRAMEWORK_REDUCE_INPUT_RECORDS] :739000
//	[MAP-REDUCE_FRAMEWORK_REDUCE_OUTPUT_RECORDS] :739000

	
	@Override
	public void process(Reader reader) throws DBException
	{
		ChukwaRecordKey key = new ChukwaRecordKey();
		ChukwaRecord record = new ChukwaRecord();
		try
		{
			StringBuilder sb = new StringBuilder();
			while (reader.next(key, record))
			{
				
				sb.append("insert into MRJobCounters ");
				for (String field :fields)
				{
					sb.append(" set ").append(field).append(" = ").append(record.getValue(field)).append(", ");
				}
				sb.append(" set timestamp =").append( record.getTime()).append(";\n");
			} 
			System.out.println(sb.toString());
		} 
		catch (Exception e)
		{
			log.error("Unable to insert data into database"
					+ e.getMessage());
			e.printStackTrace();
		} 

	}

}
