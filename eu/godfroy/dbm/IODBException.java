package eu.godfroy.dbm;

public class IODBException extends DBException
{
	public IODBException()
	{
		this(null, null);
	}

	public IODBException(String message)
	{
		this(message, null);
	}

	public IODBException(Throwable cause)
	{
		this(null, cause);
	}

	public IODBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
