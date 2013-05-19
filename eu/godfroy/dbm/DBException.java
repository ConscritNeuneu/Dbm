package eu.godfroy.dbm;

/**
 * Superclass of all the exceptions thrown by the {@link Dbm} class.
 *
 * The constructors of this class just call the constructors of its
 * superclass.
 *
 * @author Quentin Godfroy <quentin@godfroy.eu>
 */
public class DBException extends Exception
{
	/**
	 * Constructs a <code>DBException</code> with <code>null</code> as its error detail message.
	 */
	public DBException()
	{
		this(null, null);
	}

	/**
	 * Constructs a <code>DBException</code> with the specified detail message.
	 */
	public DBException(String message)
	{
		this(message, null);
	}

	/**
	 * Constructs a <code>DBException</code> with the specified cause.
	 */
	public DBException(Throwable cause)
	{
		this(null, cause);
	}

	/**
	 * Constructs a <code>IODBException</code> with the specified detail and cause.
	 */
	public DBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
