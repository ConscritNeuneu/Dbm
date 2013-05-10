package eu.godfroy.dbm;

import java.lang.ref.*;
import java.io.*;
import java.nio.*;
import java.util.*;

class PagPage
{
	public static final int PAGFILE_PGSZ = 1024;

	private static class Datum
	{
		public final byte[] content;

		public Datum(byte[] data)
		{
			content = data;
		}

		public boolean equals(Object otherObject)
		{
			if (otherObject instanceof Datum)
				return Arrays.equals(content, ((Datum) otherObject).content);

			return false;
		}

		public int hashCode()
		{
			return Arrays.hashCode(content);
		}
	};

	private final RandomAccessFile pagFile;
	private final long pagNum;
	/* in octets : 2 + Sum_entries( 4 + key.length + data.length ) */
	private int totalSize;
	private boolean isDirty;
	private final Map<Datum,Datum> keyMap;

	public PagPage(RandomAccessFile pagFile, long pagNum)
	throws IOException
	{
		this.pagFile = pagFile;
		this.pagNum = pagNum;
		totalSize = 2;
		isDirty = false;
		keyMap = new HashMap<Datum,Datum>();

		byte[] content = new byte[PAGFILE_PGSZ];
		pagFile.seek(pagNum * PAGFILE_PGSZ);
		try
		{
			pagFile.readFully(content);
			ByteBuffer contentBuf = ByteBuffer.wrap(content);
			contentBuf.order(ByteOrder.LITTLE_ENDIAN);

			int elements = contentBuf.getShort();
			int lastPosition = PAGFILE_PGSZ;
			byte[] currentKey = null;
			for (int i = 0; i < elements; i++)
			{
				int nextPosition =  contentBuf.getShort();
				byte[] data = new byte[lastPosition-nextPosition];
				System.arraycopy(content, nextPosition, data, 0, data.length);

				if (i % 2 == 0)
					currentKey = data;
				else
					keyMap.put(new Datum(currentKey),new Datum(data));

				lastPosition = nextPosition;
				totalSize += data.length + 2;
			}
		}
		catch (EOFException exception)
		{
			;
		}
	}

	public void writePage()
	throws IOException
	{
		if (isDirty)
		{
			byte[] content = new byte[PAGFILE_PGSZ];
			ByteBuffer contentBuf = ByteBuffer.wrap(content);
			contentBuf.order(ByteOrder.LITTLE_ENDIAN);

			contentBuf.putShort((short) (keyMap.size() * 2));
			int lastPosition = PAGFILE_PGSZ;
			for (Map.Entry<Datum,Datum> pair : keyMap.entrySet())
			{
				for (int i = 0; i < 2; i++)
				{
					byte[] data;

					if (i % 2 == 0)
						data = pair.getKey().content;
					else
						data = pair.getValue().content;

					int nextPosition = lastPosition - data.length;
					System.arraycopy(data, 0, content, nextPosition, data.length);
					contentBuf.putShort((short) nextPosition);

					lastPosition = nextPosition;
				}
			}
			synchronized(pagFile)
			{
				pagFile.seek(pagNum * PAGFILE_PGSZ);
				pagFile.write(content);
			}
			isDirty = false;
		}
	}

	protected void finalize()
	throws Throwable
	{
		writePage();
		super.finalize();
	}

	public byte[] fetchKey(byte[] key)
	{
		return keyMap.get(new Datum(key)).content;
	}

	public boolean writeKey(byte[] key, byte[] value)
	{
		if (key.length + value.length + 6 > PAGFILE_PGSZ)
			throw new IllegalArgumentException("Will never be able to insert: key+value too long!");

		Datum originalValue = keyMap.get(new Datum(key));
		if (originalValue != null)
		{
			if (totalSize - originalValue.content.length + value.length <= PAGFILE_PGSZ)
			{
				keyMap.put(new Datum(key), new Datum(value));
				totalSize += value.length - originalValue.content.length;
				isDirty = true;
				return true;
			}
		}
		else
		{
			if (totalSize + 4 + key.length + value.length <= PAGFILE_PGSZ)
			{
				keyMap.put(new Datum(key), new Datum(value));
				totalSize += 4 + key.length + value.length;
				isDirty = true;
				return true;
			}
		}

		/* need split */
		return false;
	}

	public void removeKey(byte[] key)
	{
		Datum datum = new Datum(key);
		if (keyMap.containsKey(datum))
		{
			byte[] value = keyMap.remove(datum).content;
			totalSize -= 4 + key.length + value.length;
			isDirty = true;
		}
	}

	public Iterable<byte[]> getAllKeys()
	{
		Set<Datum> keySet = keyMap.keySet();
		final Iterator<Datum> internalIterator = keySet.iterator();

		return new Iterable<byte[]>()
		{
			public Iterator<byte[]> iterator()
			{
				return new Iterator<byte[]>()
				{
					public boolean hasNext()
					{
						return internalIterator.hasNext();
					}

					public byte[] next()
					{
						return internalIterator.next().content;
					}

					public void remove()
					{
						throw new UnsupportedOperationException("Cannot remove a key this way!");
					}
				};
			}
		};
	}

	public void clear()
	{
		totalSize = 2;
		keyMap.clear();
		isDirty = true;
	}

	public boolean isDirty()
	{
		return isDirty;
	}
}

class DirPage
{
	public static final int DIRFILE_PGSZ = 4096;

	private final RandomAccessFile dirFile;
	private final long pagNum;
	private boolean isDirty;
	private final byte[] data;

	public DirPage(RandomAccessFile dirFile, long pagNum)
	throws IOException
	{
		this.dirFile = dirFile;
		this.pagNum = pagNum;
		isDirty = false;

		data = new byte[DIRFILE_PGSZ];
		dirFile.seek(pagNum * DIRFILE_PGSZ);
		try
		{
			dirFile.readFully(data);
		}
		catch (EOFException exception)
		{
			;
		}
	}

	public void writePage()
	throws IOException
	{
		if (isDirty)
		{
			synchronized(dirFile)
			{
				dirFile.seek(pagNum * DIRFILE_PGSZ);
				dirFile.write(data);
			}
		}
	}

	protected void finalize()
	throws Throwable
	{
		writePage();
		super.finalize();
	}

	public boolean getBit(long bitNum)
	{
		long localBit = bitNum - pagNum * DIRFILE_PGSZ * 8;

		if (localBit < 0 || localBit >= DIRFILE_PGSZ * 8)
			throw new IllegalArgumentException("Wrong DirPage!");

		return ((((data[(int) localBit/8])>>(localBit%8))&1) == 1);
	}

	public void setBit(long bitNum)
	{
		long localBit = bitNum - pagNum * DIRFILE_PGSZ * 8;

		 if (localBit < 0 || localBit >= DIRFILE_PGSZ * 8)
			throw new IllegalArgumentException("Wrong DirPage!");

		data[(int) localBit/8] |= (1 << (localBit % 8));

		isDirty = true;
	}
}

public class Dbm
{
	private static final String PAG_EXT = ".pag";
	private static final String DIR_EXT = ".dir";

	private final RandomAccessFile pagFile;
	private final RandomAccessFile dirFile;

	/* also hold a PhantomReference to clear the mapping */
	private final Map<Long,Reference<PagPage>> pagPages;
	private final Map<Long,Reference<DirPage>> dirPages;

	public Dbm(String database)
	throws IOException
	{
		File pagF = new File(database + PAG_EXT);
		File dirF = new File(database + DIR_EXT);

		pagFile = new RandomAccessFile(pagF, "rw");
		dirFile = new RandomAccessFile(dirF, "rw");

		pagPages = new TreeMap<Long,Reference<PagPage>>();
		dirPages = new TreeMap<Long,Reference<DirPage>>();
	}

	private static byte[] hitab = new byte[] {
		61, 57, 53, 49, 45, 41, 37, 33,
		29, 25, 21, 17, 13, 9, 5, 1
	};

	private static int[] hltab = new int[] {
		0x3100d2bf, 0x3118e3de, 0x34ab1372, 0x2807a847,
		0x1633f566, 0x2143b359, 0x26d56488, 0x3b9e6f59,
		0x37755656, 0x3089ca7b, 0x18e92d85, 0x0cd0e9d8,
		0x1a9e3b54, 0x3eaa902f, 0x0d9bfaae, 0x2f32b45b,
		0x31ed6102, 0x3d3c8398, 0x146660e3, 0x0f8d4b76,
		0x02c77a5f, 0x146c8799, 0x1c47f51f, 0x249f8f36,
		0x24772043, 0x1fbc1e4d, 0x1e86b3fa, 0x37df36a6,
		0x16ed30e4, 0x02c3148e, 0x216e5929, 0x0636b34e,
		0x317f9f56, 0x15f09d70, 0x131026fb, 0x38c784b1,
		0x29ac3305, 0x2b485dc5, 0x3c049ddc, 0x35a9fbcd,
		0x31d5373b, 0x2b246799, 0x0a2923d3, 0x08a96e9d,
		0x30031a9f, 0x08f525b5, 0x33611c06, 0x2409db98,
		0x0ca4feb2, 0x1000b71e, 0x30566e32, 0x39447d31,
		0x194e3752, 0x08233a95, 0x0f38fe36, 0x29c7cd57,
		0x0f7b3a39, 0x328e8a16, 0x1e7d1388, 0x0fba78f5,
		0x274c7e7c, 0x1e8be65c, 0x2fa0b0bb, 0x1eb6c371
	};

	private static int computeHash(byte[] key)
	{
		byte hashi = 0;
		int hashl = 0;
		for (byte elem : key)
		{
			for (int i = 0; i < 2; i++)
			{
				hashi += hitab[elem & (hitab.length - 1)];
				hashl += hltab[hashi & (hltab.length - 1)];
				elem >>= 4;
			}
		}

		return hashl;
	}

	private PagPage getPagPage(long pagNum)
	throws IOException
	{
		PagPage page = null;
		Reference<PagPage> ref = pagPages.get(pagNum);
		if (ref != null)
			page = ref.get();
		if (page == null)
		{
			page = new PagPage(pagFile, pagNum);
			pagPages.put(pagNum, new SoftReference<PagPage>(page));
		}

		return page;
	}

	private DirPage getDirPage(long pagNum)
	throws IOException
	{
		DirPage page = null;
		Reference<DirPage> ref = dirPages.get(pagNum);
		if (ref != null)
			page = ref.get();
		if (page == null)
		{
			page = new DirPage(dirFile, pagNum);
			dirPages.put(pagNum, new SoftReference<DirPage>(page));
		}

		return page;
	}

	private boolean isSplit(int mask, long pagNum)
	throws IOException
	{
		long bitNum = (mask & 0xffffffffl) + pagNum;
		DirPage page = getDirPage(bitNum / (8 * DirPage.DIRFILE_PGSZ));
		return page.getBit(bitNum);
	}

	private void markSplit(int mask, long pagNum)
	throws IOException
	{
		long bitNum = (mask & 0xffffffffl) + pagNum;
		DirPage page = getDirPage(bitNum / (8 * DirPage.DIRFILE_PGSZ));
		page.setBit(bitNum);
		page.writePage();
	}

	private void splitPage(int mask, long pagNum)
	throws IOException,
	       DBException
	{
		if (mask == -1)
			throw new IllegalArgumentException("Cannot split anymore!");

		PagPage pagPage = getPagPage(pagNum);
		long newPagNum = pagNum | ((mask + 1) & 0xffffffffl);
		PagPage newPagPage = getPagPage(newPagNum);

		List<byte[]> keys = new ArrayList<byte[]>();
		List<byte[]> values = new ArrayList<byte[]>();
		for (byte[] key : pagPage.getAllKeys())
		{
			byte[] value = pagPage.fetchKey(key);
			keys.add(key);
			values.add(value);
		}
		pagPage.clear();

		int newMask = (mask << 1) + 1;

		for (int i = 0; i < keys.size(); i++)
		{
			byte[] key = keys.get(i);
			byte[] value = values.get(i);

			int hash = computeHash(key);
			long page = (hash & newMask) & 0xffffffffl;
			if (page != pagNum && page != newPagNum)
				throw new CorruptedDBException("Content pair is not in right page!");
			if (page == pagNum)
				pagPage.writeKey(key, value);
			else
				newPagPage.writeKey(key, value);
		}

		markSplit(mask, pagNum);
		pagPage.writePage();
		newPagPage.writePage();
	}

	public byte[] get(byte[] key)
	throws IOException
	{
		int mask = 0;
		int hash = computeHash(key);
		while (isSplit(mask, hash & mask))
			mask = (mask << 1) + 1;
		PagPage pagPage = getPagPage(hash & mask);
		return pagPage.fetchKey(key);
	}

	public void put(byte[] key, byte[] value)
	throws IOException,
	       DBException
	{
		int mask = 0;
		int hash = computeHash(key);
		while (isSplit(mask, hash & mask))
			mask = (mask << 1) + 1;
		PagPage pagPage = getPagPage(hash & mask);
		while (!pagPage.writeKey(key, value) && mask != -1)
		{
			splitPage(mask, hash & mask);
			mask = (mask << 1) + 1;
			pagPage = getPagPage(hash & mask);
		}
		if (mask == -1)
			throw new IllegalArgumentException("Cannot insert key!");
		else
			pagPage.writePage();
	}

	public class AllKeysGetter
	{
		private int hash;
		private int mask;
		private Iterator<byte[]> pageIterator;

		private AllKeysGetter()
		throws IOException
		{
			while (isSplit(mask, hash & mask))
				mask = (mask << 1) + 1;
			pageIterator = getPagPage(hash & mask).getAllKeys().iterator();
		}

		public byte[] nextKey()
		throws IOException
		{
			while (!pageIterator.hasNext())
			{
				if ((hash | ~mask) == -1)
					return null;

				/* set the highest possible bit which isn't
				 * already set, but within the mask. The
				 * higher bits are not important since the
				 * page wasn't split */
				hash &= mask;
				int bit;
				if (mask != -1)
					bit = (mask + 1) >> 1;
				else
					bit = 1 << 31;
				while ((hash & bit) != 0)
				{
					hash &= ~bit;
					bit >>= 1;
				}
				hash |= bit;
				/* set the mask to this value, no need to
				 * reset it to 0, the lower bits are used
				 * for sure */
				mask = (bit << 1) - 1;
				while (isSplit(mask, hash & mask))
					mask = (mask << 1) + 1;

				pageIterator = getPagPage(hash & mask).getAllKeys().iterator();
			}
			return pageIterator.next();
		}
	}

	public AllKeysGetter getAll()
	throws IOException
	{
		return new AllKeysGetter();
	}
}
