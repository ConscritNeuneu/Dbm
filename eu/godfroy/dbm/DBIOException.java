package eu.godfroy.dbm;

public class DBIOException extends RuntimeException
{
	public DBIOException()
	{
		this(null, null);
	}

	public DBIOException(String message)
	{
		this(message, null);
	}

	public DBIOException(Throwable cause)
	{
		this(null, cause);
	}

	public DBIOException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
