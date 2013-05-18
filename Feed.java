import java.io.*;
import eu.godfroy.dbm.*;

public class Feed
{
	public static void main(String[] args)
	throws IOException,
	       DBException
	{
		if (args.length < 1)
		{
			System.err.println("Usage: Feed database");
			System.exit(1);
		}

		Dbm dataBase = new Dbm(args[0]);

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String line;
		int linesRead = 0;
		boolean unfinishedLineInfo = false;
		while ((line = reader.readLine()) != null)
		{
			String[] keyval = line.split("\t");
			dataBase.put(keyval[0].getBytes(), keyval[1].getBytes());
			linesRead++;
			if (linesRead % 100 == 0)
			{
				System.out.print('.');
				unfinishedLineInfo = true;
			}
			if (linesRead % 7000 == 0)
			{
				System.out.println("");
				unfinishedLineInfo = false;
			}
		}
		if (unfinishedLineInfo)
			System.out.println("");
	}
}
