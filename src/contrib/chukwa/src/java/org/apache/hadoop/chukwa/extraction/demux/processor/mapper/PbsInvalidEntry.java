package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

public class PbsInvalidEntry extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 9154096600390233023L;

	public PbsInvalidEntry()
	{}

	public PbsInvalidEntry(String message)
	{
		super(message);
	}

	public PbsInvalidEntry(Throwable cause)
	{
		super(cause);
	}

	public PbsInvalidEntry(String message, Throwable cause)
	{
		super(message, cause);
	}

}
