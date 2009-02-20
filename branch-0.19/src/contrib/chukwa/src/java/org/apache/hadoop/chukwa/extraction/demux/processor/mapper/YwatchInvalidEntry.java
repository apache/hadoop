package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

public class YwatchInvalidEntry extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7074989443687516732L;

	public YwatchInvalidEntry()
	{
	}

	public YwatchInvalidEntry(String message)
	{
		super(message);
	}

	public YwatchInvalidEntry(Throwable cause)
	{
		super(cause);
	}

	public YwatchInvalidEntry(String message, Throwable cause)
	{
		super(message, cause);
	}

}
