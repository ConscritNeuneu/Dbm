import eu.godfroy.dbm.*;

public class GetAll
{
	public static void main(String[] args)
	throws java.io.IOException,
	       DBException
	{
		if (args.length < 1)
		{
			System.out.println("usage: GetAll database");
			System.exit(1);
		}

		Dbm dataBase = new Dbm(args[0], "r");

		for (byte[] key : dataBase.allKeys())
		{
			byte[] value = dataBase.get(key);
			System.out.println(new String(key) + "\t" + new String(value));
		}
	}
}
