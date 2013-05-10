package eu.godfroy.dbm;

public class CorruptedDBException extends DBException
{
	public CorruptedDBException()
	{
		this(null, null);
	}

	public CorruptedDBException(String message)
	{
		this(message, null);
	}

	public CorruptedDBException(Throwable cause)
	{
		this(null, cause);
	}

	public CorruptedDBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
