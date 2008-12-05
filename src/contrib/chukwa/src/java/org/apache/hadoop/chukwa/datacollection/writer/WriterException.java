package org.apache.hadoop.chukwa.datacollection.writer;

public class WriterException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4207275200546397145L;

	public WriterException()
	{}

	public WriterException(String message)
	{
		super(message);
	}

	public WriterException(Throwable cause)
	{
		super(cause);
	}

	public WriterException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
