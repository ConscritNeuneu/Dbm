package eu.godfroy.dbm;

/**
 * Exception thrown by the {@link Dbm} class when encountering a non
 * recoverable error in the database.
 *
 * The constructors of this class just call the constructors of its
 * superclass.
 *
 * @author Quentin Godfroy <quentin@godfroy.eu>
 */
public class CorruptedDBException extends DBException
{
	/**
	 * Constructs a <code>CorruptedDBException</code> with <code>null</code> as
	 * its error detail message.
	 */
	public CorruptedDBException()
	{
		this(null, null);
	}

	/**
	 * Construcs a <code>CorruptedDBException</code> with the specified
	 * detail message.
	 */
	public CorruptedDBException(String message)
	{
		this(message, null);
	}

	/**
	 * Construcs a <code>CorruptedDBException</code> with the specified
	 * cause.
	 */
	public CorruptedDBException(Throwable cause)
	{
		this(null, cause);
	}

	/**
	 * Construcs a <code>CorruptedDBException</code> with the specified
	 * detail and cause.
	 */
	public CorruptedDBException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
