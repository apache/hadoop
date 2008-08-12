package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

public class UnknownRecordTypeException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8925135975093252279L;

	public UnknownRecordTypeException()
	{}

	public UnknownRecordTypeException(String message)
	{
		super(message);
	}

	public UnknownRecordTypeException(Throwable cause)
	{
		super(cause);
	}

	public UnknownRecordTypeException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
