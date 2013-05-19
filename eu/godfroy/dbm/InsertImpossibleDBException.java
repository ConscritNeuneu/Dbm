package eu.godfroy.dbm;

/**
 * Exception thrown by the {@link Dbm} class when it cannot insert the key into the database.
 *
 * The constructors of this class just call the constructors of its superclass. 
 *
 * @author Quentin Godfroy <quentin@godfroy.eu>
 */
public class InsertImpossibleDBException extends DBException
{
	/**
	 * Constructs an <code>InsertImpossibleDBException</code> with
	 * <code>null</code> as its error detail message.
	 */
	public InsertImpossibleDBException()
	{
		this(null, null);
	}

	/**
	 * Constructs an <code>InsertImpossibleDBException</code> with the
	 * specified detail message.
	 */
	public InsertImpossibleDBException(String message)
	{
		this(message, null);
	}

	/**
	 * Constructs an <code>InsertImpossibleDBException</code> with the
	 * specified cause.
	 */
	public InsertImpossibleDBException(Throwable cause)
	{
		this(null, cause);
	}

	/**
	 * Constructs an <code>InsertImpossibleDBException</code> with the
	 * specified detail and cause.
	 */
	public InsertImpossibleDBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
