package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

public class DuplicateProcessorException extends RuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3890267797961057789L;

	public DuplicateProcessorException()
	{}

	public DuplicateProcessorException(String message)
	{
		super(message);
	}

	public DuplicateProcessorException(Throwable cause)
	{
		super(cause);
	}

	public DuplicateProcessorException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
