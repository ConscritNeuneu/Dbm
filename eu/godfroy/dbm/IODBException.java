package eu.godfroy.dbm;

/**
 * Exception thrown by the {@link Dbm} class when it encounters an IOException.
 *
 * The constructors of this class just call the constructors of its
 * superclass.
 *
 * @author Quentin Godfroy <quentin@godfroy.eu>
 */
public class IODBException extends DBException
{
	/**
	 * Constructs an <code>IODBException</code> with <code>null</code> as its error detail message.
	 */
	public IODBException()
	{
		this(null, null);
	}

	/**
	 * Constructs an <code>IODBException</code> with the specified detail message.
	 */
	public IODBException(String message)
	{
		this(message, null);
	}

	/**
	 * Constructs an <code>IODBException</code> with the specified cause.
	 */
	public IODBException(Throwable cause)
	{
		this(null, cause);
	}

	/**
	 * Constructs an <code>IODBException</code> with the specified detail and cause.
	 */
	public IODBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
