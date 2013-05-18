import eu.godfroy.dbm.*;

public class GetAll2
{
	private static void printKeyVal(byte[] key, byte[] value)
	{
		System.out.println(new String(key) + "\t" + new String(value));
	}

	public static void main(String[] args)
	throws java.io.IOException,
	       DBException
	{
		if (args.length < 1)
		{
			System.err.println("Usage: GetAll2 database");
			System.exit(1);
		}

		Dbm dataBase = new Dbm(args[0], "r");

		byte[] key = dataBase.firstKey();
		byte[] value = dataBase.get(key);
		printKeyVal(key, value);
		while ((key = dataBase.nextKey(key)) != null)
		{
			value = dataBase.get(key);
			printKeyVal(key, value);
		}
	}
}
