package eu.godfroy.dbm;

import java.lang.ref.*;
import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * A public domain reimplementation of the DBM package from Unix v7.
 *
 * The files are binary compatible with the original implementation, as well
 * as the ones produced by the NDBM package originated in
 * 4.3BSD, and used in various unices since then.
 * <p>
 * This class allows to insert arbitrary pairs of key, value represented as
 * <code>byte[]</code>, of combined length no more than 1018 octets. It has
 * no provision for overflow pages, and keys that hash together must fit in
 * a single page.
 * <p>
 * A database is comprised of two files: <code>database.pag</code> which
 * contains the pages with the key, values inserted, and
 * <code>database.dir</code>, which is an index of the pages.
 * <p>
 * The produced files are sparse, and care must be taken when copying them
 * using traditional Unix tools. While not detrimental to the database
 * integrity, a dumb copy can provoke a large increase in the effective size
 * taken by the files on the filesystem.
 *
 * @author Quentin Godfroy <quentin@godfroy.eu>
 */
public class Dbm
{
	private static final String PAG_EXT = ".pag";
	private static final String DIR_EXT = ".dir";

	/**
	 * Enum which represents the two endianness.
	 */
	public static enum Endianness
	{
		BIG_ENDIAN(ByteOrder.BIG_ENDIAN),
		LITTLE_ENDIAN(ByteOrder.LITTLE_ENDIAN);

		private final ByteOrder order;

		Endianness(ByteOrder order)
		{
			this.order = order;
		}

		private ByteOrder getEndianness()
		{
			return order;
		}
	}

	private final RandomAccessFile pagFile;
	private final RandomAccessFile dirFile;

	private final ByteOrder endianness;

	/* also hold a PhantomReference to clear the mapping */
	private final Map<Long,Reference<PagPage>> pagPages;
	private final Map<Long,Reference<DirPage>> dirPages;

	private static class Datum
	implements Comparable<Datum>
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

		public int compareTo(Datum otherDatum)
		{
			if (content.length != otherDatum.content.length)
				return content.length - otherDatum.content.length;

			for (int i = 0; i < content.length; i++)
			{
				if (content[i] != otherDatum.content[i])
					return (content[i] - otherDatum.content[i]);
			}

			return 0;
		}
	};

	private class PagPage
	{
		private static final int PAGFILE_PGSZ = 1024;

		private final long pagNum;
		/* in octets : 2 + Sum_entries( 4 + key.length + data.length ) */
		private int totalSize;
		private boolean isDirty;
		private final Map<Datum,Datum> keyMap;

		private PagPage(long pagNum)
		throws DBException
		{
			this.pagNum = pagNum;
			totalSize = 2;
			isDirty = false;
			keyMap = new HashMap<Datum,Datum>();

			byte[] content = new byte[PAGFILE_PGSZ];
			try
			{
				pagFile.seek(pagNum * PAGFILE_PGSZ);
				try
				{
					pagFile.readFully(content);
					ByteBuffer contentBuf = ByteBuffer.wrap(content);
					contentBuf.order(endianness);

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
				catch (IndexOutOfBoundsException exception)
				{
					keyMap.clear();
					totalSize = 2;
					throw new CorruptedDBException("Corrupted page " + pagNum, exception);
				}
			}
			catch (IOException exception)
			{
				throw new IODBException(exception);
			}
		}

		private void writePage()
		throws DBException
		{
			if (isDirty)
			{
				byte[] content = new byte[PAGFILE_PGSZ];
				ByteBuffer contentBuf = ByteBuffer.wrap(content);
				contentBuf.order(endianness);

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
				try
				{
					synchronized(pagFile)
					{
						pagFile.seek(pagNum * PAGFILE_PGSZ);
						pagFile.write(content);
					}
				}
				catch (IOException exception)
				{
					throw new IODBException(exception);
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

		private byte[] fetchKey(byte[] key)
		{
			Datum value = keyMap.get(new Datum(key));
			return (value != null) ? value.content : null;
		}

		private boolean writeKey(byte[] key, byte[] value)
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

		private byte[] removeKey(byte[] key)
		{
			Datum datum = new Datum(key);

			byte[] value = null;
			if (keyMap.containsKey(datum))
			{
				value = keyMap.remove(datum).content;
				totalSize -= 4 + key.length + value.length;
				isDirty = true;
			}

			return value;
		}

		private Iterable<byte[]> getAllKeys()
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

		private byte[] getNextKey(byte[] previousKey)
		{
			Datum previousKeyDatum = (previousKey != null) ? new Datum(previousKey) : null;

			Datum selectedNextDatum = null;
			for (Datum otherKeyDatum : keyMap.keySet())
			{
				if ((previousKeyDatum == null || otherKeyDatum.compareTo(previousKeyDatum) < 0) &&
				    (selectedNextDatum == null || otherKeyDatum.compareTo(selectedNextDatum) > 0))
					selectedNextDatum = otherKeyDatum;
			}

			if (selectedNextDatum != null)
				return selectedNextDatum.content;

			return null;
		}

		private void clear()
		{
			totalSize = 2;
			keyMap.clear();
			isDirty = true;
		}

		private boolean isDirty()
		{
			return isDirty;
		}
	}

	private class DirPage
	{
		public static final int DIRFILE_PGSZ = 4096;

		private final long pagNum;
		private boolean isDirty;
		private final byte[] data;

		public DirPage(long pagNum)
		throws DBException
		{
			this.pagNum = pagNum;
			isDirty = false;

			data = new byte[DIRFILE_PGSZ];
			try
			{
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
			catch (IOException exception)
			{
				throw new IODBException(exception);
			}
		}

		public void writePage()
		throws DBException
		{
			if (isDirty)
			{
				try
				{
					synchronized(dirFile)
					{
						dirFile.seek(pagNum * DIRFILE_PGSZ);
						dirFile.write(data);
					}
				}
				catch (IOException exception)
				{
					throw new IODBException(exception);
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

	/**
	 * Connect to the database with the specified file options and
	 * endianness.
	 *
	 * Files <code>database + ".pag"</code> and <code>database +
	 * ".dir"</code> will be opened as {@link java.io.RandomAccessFile}
	 * with mode as specified by <code>fileOptions</code>. The database
	 * will be opened using the specified endianness.
	 *
	 * @param database The files <code>database + ".pag"</code> and
	 * <code>database + ".dir"</code> will be attempted to be opened.
	 * @param fileOptions Mode of opening, as specified by the
	 * <code>mode</code> of {@link java.io.RandomAccessFile}.
	 * @param endianness Either {@link Endianness#LITTLE_ENDIAN}
	 * or {@link Endianness#BIG_ENDIAN}.
	 */
	public Dbm(String database, String fileOptions, Endianness endianness)
	throws IOException
	{
		File pagF = new File(database + PAG_EXT);
		File dirF = new File(database + DIR_EXT);

		pagFile = new RandomAccessFile(pagF, fileOptions);
		dirFile = new RandomAccessFile(dirF, fileOptions);

		this.endianness = endianness.getEndianness();

		pagPages = new TreeMap<Long,Reference<PagPage>>();
		dirPages = new TreeMap<Long,Reference<DirPage>>();
	}

	/**
	 * Connect to the database with the specified file options.
	 *
	 * Files <code>database + ".pag"</code> and <code>database + ".dir"</code> will be opened as
	 * {@link java.io.RandomAccessFile} with mode as specified by
	 * <code>fileOptions</code>. The database will be assumed to be
	 * in little endian format. To specify the endianness, see {@link #Dbm(String, String, Endianness)}.
	 *
	 * @param database The files <code>database + ".pag"</code> and <code>database
	 * + ".dir"</code> will be attempted to be opened.
	 * @param fileOptions Mode of opening, as specified by the
	 * <code>mode</code> of {@link java.io.RandomAccessFile}.
	 */
	public Dbm(String database, String fileOptions)
	throws IOException
	{
		this(database, fileOptions, Endianness.LITTLE_ENDIAN);
	}

	/** Connect to a database with default options.
	 *
	 * See the other constructors for more options.
	 */
	public Dbm(String database)
	throws IOException
	{
		this(database, "rw");
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
	throws DBException
	{
		PagPage page = null;
		Reference<PagPage> ref = pagPages.get(pagNum);
		if (ref != null)
			page = ref.get();
		if (page == null)
		{
			page = new PagPage(pagNum);
			pagPages.put(pagNum, new SoftReference<PagPage>(page));
		}

		return page;
	}

	private DirPage getDirPage(long pagNum)
	throws DBException
	{
		DirPage page = null;
		Reference<DirPage> ref = dirPages.get(pagNum);
		if (ref != null)
			page = ref.get();
		if (page == null)
		{
			page = new DirPage(pagNum);
			dirPages.put(pagNum, new SoftReference<DirPage>(page));
		}

		return page;
	}

	private boolean isSplit(int mask, long pagNum)
	throws DBException
	{
		long bitNum = (mask & 0xffffffffl) + pagNum;
		DirPage page = getDirPage(bitNum / (8 * DirPage.DIRFILE_PGSZ));
		return page.getBit(bitNum);
	}

	private void markSplit(int mask, long pagNum)
	throws DBException
	{
		long bitNum = (mask & 0xffffffffl) + pagNum;
		DirPage page = getDirPage(bitNum / (8 * DirPage.DIRFILE_PGSZ));
		page.setBit(bitNum);
		page.writePage();
	}

	private void splitPage(int mask, long pagNum)
	throws DBException
	{
		if (mask == -1)
			throw new InsertImpossibleDBException("Cannot split anymore!");

		PagPage pagPage = getPagPage(pagNum);
		long newPagNum = pagNum | ((mask + 1) & 0xffffffffl);
		PagPage newPagPage = getPagPage(newPagNum);
		if (newPagPage.totalSize != 2)
			throw new CorruptedDBException("Page " + newPagNum + " is not empty!");

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
				throw new CorruptedDBException("Content pair from page " + pagNum + " is not in right page!");
			if (page == pagNum)
				pagPage.writeKey(key, value);
			else
				newPagPage.writeKey(key, value);
		}

		newPagPage.writePage();
		markSplit(mask, pagNum);
		pagPage.writePage();
	}

	/**
	 * Get the value associated with key.
	 *
	 * @param key Key to be searched for.
	 * @return Value associated with key if key exists, else
	 * <code>null</code>.
	 * @throws CorruptedDBException if the database is corrupted or
	 * opened with the wrong endianness.
	 * @throws IODBException in case the reads on either backing file
	 * produces an {@link java.io.IOException}.
	 */
	public byte[] get(byte[] key)
	throws DBException
	{
		int mask = 0;
		int hash = computeHash(key);
		while (isSplit(mask, hash & mask))
			mask = (mask << 1) + 1;
		PagPage pagPage = getPagPage(hash & mask);
		return pagPage.fetchKey(key);
	}

	/**
	 * Insert a key, value pair into the database.
	 *
	 * @param key Key to be inserted.
	 * @param value Value to be inserted.
	 *
	 * @throws CorruptedDBException if the database is corrupted or
	 * opened with the wrong endianness.
	 * @throws IODBException in case one of the reads or write on either
	 * backing files produce an {@link java.io.IOException}. In particular this happens
	 * if the database has been opened read-only.
	 * @throws InsertImpossibleDBException when the insert failed for a
	 * reason inherent to the DBM format. For instance, if
	 * <code>key.length + value.length &gt; 1018</code> or if the key
	 * hashes together with another different key already in database
	 * and their combined length exceed the maximum admissible size.
	 */
	public void put(byte[] key, byte[] value)
	throws DBException
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
			throw new InsertImpossibleDBException("Cannot insert key!");
		else
			pagPage.writePage();
	}

	/**
	 * Remove a key from the map.
	 *
	 * @param key Key to be removed.
	 * @return The value associated with key if it exists.
	 * <code>null</code> instead.
	 * @throws CorruptedDBException if the database is corrupted or
	 * opened with the wrong endianness.
	 * @throws IODBException in case one of the reads or write on either
	 * backing files produce an {@link java.io.IOException}. In particular this
	 * happensif the database has been opened read-only.
	 */
	public byte[] remove(byte[] key)
	throws DBException
	{
		int mask = 0;
		int hash = computeHash(key);
		while (isSplit(mask, hash & mask))
			mask = (mask << 1) + 1;
		PagPage pagPage = getPagPage(hash & mask);
		byte[] data = pagPage.removeKey(key);
		if (data != null)
			pagPage.writePage();
		return data;
	}

	private static class HashMask
	{
		private final int hash;
		private final int mask;

		private HashMask(int hash, int mask)
		{
			this.hash = hash;
			this.mask = mask;
		}
	}

	private HashMask hashInc(int hash, int mask)
	throws DBException
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

		return new HashMask(hash, mask);
	}

	/**
	 * Returns the next key following the provided key passed in
	 * parameter.
	 *
	 * The keys are returned in the same order as the original DBM
	 * library. This method is completely stateless.
	 *
	 * @param key The last returned key.
	 * @return The next following key, if it exists. A value of
	 * <code>null</code> indicated that no more keys are available.
	 * @throws CorruptedDBException if the database is corrupted or opened
	 * with the wrong endianness.
	 * @throws IODBException in case one of the reads on either backing
	 * file produced an {@link java.io.IOException}.
	 */
	public byte[] nextKey(byte[] key)
	throws DBException
	{
		int mask = 0;
		int hash = (key != null) ? computeHash(key) : 0;
		while (isSplit(mask, hash & mask))
			mask = (mask << 1) + 1;

		PagPage currentPage = getPagPage(hash & mask);
		byte[] next;
		while ((next = currentPage.getNextKey(key)) == null)
		{
			HashMask hashMask = hashInc(hash, mask);
			if (hashMask == null)
				return null;

			hash = hashMask.hash;
			mask = hashMask.mask;
			currentPage = getPagPage(hash & mask);
			key = null;
		}
		return next;
	}

	/**
	 * Obtain the first key.
	 *
	 * Call this method in order to start a traversal of all the keys.
	 *
	 * @return The first key of the database. <code>null</code> if the
	 * database is empty.
	 * @throws CorruptedDBException if the database is corrupted or
	 * opened with the wrong endianness.
	 * @throws IODBException in case one of the reads on either backing
	 * file produced an {@link java.io.IOException}.
	 */
	public byte[] firstKey()
	throws DBException
	{
		return nextKey(null);
	}

	private class AllKeysGetter
	{
		private int hash;
		private int mask;
		private Iterator<byte[]> pageIterator;

		private AllKeysGetter()
		throws DBException
		{
			while (isSplit(mask, hash & mask))
				mask = (mask << 1) + 1;
			pageIterator = getPagPage(hash & mask).getAllKeys().iterator();
		}

		public byte[] nextKey()
		throws DBException
		{
			while (!pageIterator.hasNext())
			{
				HashMask hashMask = hashInc(hash, mask);
				if (hashMask == null)
					return null;

				hash = hashMask.hash;
				mask = hashMask.mask;

				pageIterator = getPagPage(hash & mask).getAllKeys().iterator();
			}
			return pageIterator.next();
		}
	}

	/**
	 * Stateful traversal method.
	 *
	 * Call this method to obtain an Iterable suitable for <code>for in</code>
	 * loops. The keys are produced in an undefined order.
	 * <p>
	 * The <code>iterable()</code> method and the respective methods of the returned
	 * iterator can throw {@link java.lang.RuntimeException}, with
	 * {@link IODBException} or {@link CorruptedDBException} as a cause.
	 * <p>
	 * The {@link IODBException} indicates that an
	 * {@link java.io.IOException} was encountered while reading the underlying
	 * files. The {@link CorruptedDBException} indicates that the database is
	 * corrupted or that it was opened with the wrong endianness.
	 *
	 * @return An iterable usable in a <code>for in</code> loop.
	 */
	/* returns a RuntimeException as a cause of an unchecked DBException */
	public Iterable<byte[]> allKeys()
	{
		return new Iterable<byte[]>()
		{
			public Iterator<byte[]> iterator()
			{
				return new Iterator<byte[]>()
				{
					boolean isNextKey;
					byte[] nextKey;
					final AllKeysGetter getter;

					{
						try
						{
							getter = new AllKeysGetter();
						}
						catch (DBException exception)
						{
							throw new RuntimeException(exception);
						}
					}

					public boolean hasNext()
					{
						if (!isNextKey)
						{
							try
							{
								nextKey = getter.nextKey();
							}
							catch (DBException exception)
							{
								throw new RuntimeException(exception);
							}
							isNextKey = true;
						}

						return nextKey != null;
					}

					public byte[] next()
					{
						if (!isNextKey)
						{
							try
							{
								nextKey = getter.nextKey();
							}
							catch (DBException exception)
							{
								throw new RuntimeException(exception);
							}
						}

						if (nextKey == null)
							throw new NoSuchElementException();

						isNextKey = false;
						return nextKey;
					}

					public void remove()
					{
						throw new UnsupportedOperationException("Cannot remove a key this way!");
					}
				};
			}
		};
	}
}
