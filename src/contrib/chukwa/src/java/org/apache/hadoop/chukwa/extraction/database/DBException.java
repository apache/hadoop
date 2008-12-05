package org.apache.hadoop.chukwa.extraction.database;

public class DBException extends Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4509384580029389936L;

	public DBException()
	{
	}

	public DBException(String message)
	{
		super(message);
	}

	public DBException(Throwable cause)
	{
		super(cause);
	}

	public DBException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
