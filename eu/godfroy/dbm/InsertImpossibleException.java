package eu.godfroy.dbm;

public class InsertImpossibleException extends DBException
{
	public InsertImpossibleException()
	{
		this(null, null);
	}

	public InsertImpossibleException(String message)
	{
		this(message, null);
	}

	public InsertImpossibleException(Throwable cause)
	{
		this(null, cause);
	}

	public InsertImpossibleException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
