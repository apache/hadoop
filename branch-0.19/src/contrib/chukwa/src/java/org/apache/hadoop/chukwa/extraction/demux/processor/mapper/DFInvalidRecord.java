package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

public class DFInvalidRecord extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1254238125122522523L;

	public DFInvalidRecord()
	{
	}

	public DFInvalidRecord(String arg0)
	{
		super(arg0);
	}

	public DFInvalidRecord(Throwable arg0)
	{
		super(arg0);
	}

	public DFInvalidRecord(String arg0, Throwable arg1)
	{
		super(arg0, arg1);
	}

}
