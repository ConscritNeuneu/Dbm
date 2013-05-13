package eu.godfroy.dbm;

public class InsertImpossibleDBException extends DBException
{
	public InsertImpossibleDBException()
	{
		this(null, null);
	}

	public InsertImpossibleDBException(String message)
	{
		this(message, null);
	}

	public InsertImpossibleDBException(Throwable cause)
	{
		this(null, cause);
	}

	public InsertImpossibleDBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
