package eu.godfroy.dbm;

public class DBException extends Exception
{
	public DBException()
	{
		this(null, null);
	}

	public DBException(String message)
	{
		this(message, null);
	}

	public DBException(Throwable cause)
	{
		this(null, cause);
	}

	public DBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
