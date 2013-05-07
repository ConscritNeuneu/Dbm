package eu.godfroy.dbm;

import java.io.*;
import java.nio.*;
import java.util.*;

class PagPage
{
	private static final int PAGFILE_PGSZ = 1024;

	private final RandomAccessFile pagFile;
	private final long pagNum;
	/* in octets : 2 + Sum_entries( 4 + key.length + data.lenght ) */
	private int totalSize;
	private boolean isDirty;
	private final Map<byte[],byte[]> keyMap;

	public PagPage(RandomAccessFile pagFile, long pagNum)
	throws IOException
	{
		this.pagFile = pagFile;
		this.pagNum = pagNum;
		totalSize = 2;
		isDirty = false;
		keyMap = new HashMap<byte[],byte[]>();

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
					keyMap.put(currentKey, data);

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
		byte[] content = new byte[PAGFILE_PGSZ];
		ByteBuffer contentBuf = ByteBuffer.wrap(content);
		contentBuf.order(ByteOrder.LITTLE_ENDIAN);
		
		contentBuf.putShort((short) (keyMap.size() * 2));
		int lastPosition = PAGFILE_PGSZ;
		for (Map.Entry<byte[],byte[]> pair : keyMap.entrySet())
		{
			for (int i = 0; i < 2; i++)
			{
				byte[] data;

				if (i % 2 == 0)
					data = pair.getKey();
				else
					data = pair.getValue();

				int nextPosition = lastPosition - data.length;
				System.arraycopy(data, 0, content, nextPosition, data.length);
				contentBuf.putShort((short) nextPosition);

				lastPosition = nextPosition;
			}
		}
		pagFile.seek(pagNum * PAGFILE_PGSZ);
		pagFile.write(content);
		isDirty = false;
	}

	public byte[] fetchKey(byte[] key)
	{
		return keyMap.get(key);
	}

	public boolean writeKey(byte[] key, byte[] value)
	{
		byte[] originalValue = keyMap.get(key);
		if (originalValue != null)
		{
			if (totalSize - originalValue.length + value.length <= PAGFILE_PGSZ)
			{
				keyMap.put(key, value);
				totalSize += value.length - originalValue.length;
				isDirty = true;
				return true;
			}
		}
		else
		{
			if (totalSize + 4 + key.length + value.length <= PAGFILE_PGSZ)
			{
				keyMap.put(key, value);
				totalSize += 4 + key.length + value.length;
				isDirty = true;
				return true;
			}
		}

		/* need split */
		return false;
	}

	public boolean isDirty()
	{
		return isDirty;
	}
}

public class Dbm
{
	private static final String PAG_EXT = ".pag";
	private static final String DIR_EXT = ".dir";

	private static final int DIRFILE_PGSZ = 4096;

	private final RandomAccessFile pagFile;
	private final RandomAccessFile dirFile;

	public Dbm(String database)
	throws IOException
	{
		File pagF = new File(database + PAG_EXT);
		File dirF = new File(database + DIR_EXT);

		pagFile = new RandomAccessFile(pagF, "rw");
		dirFile = new RandomAccessFile(dirF, "rw");
	}

	private byte[] readDirPage(long pagNum)
	throws IOException
	{
		byte[] page = new byte[DIRFILE_PGSZ];
		dirFile.seek(pagNum * DIRFILE_PGSZ);
		try
		{
			dirFile.readFully(page);
		}
		catch (EOFException exception)
		{
			;
		}

		return page;
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

	public static void main(String[] args)
	throws IOException
	{
		RandomAccessFile pagFile = new RandomAccessFile("phones.pag", "rw");
		PagPage page = new PagPage(pagFile, 0l);
		page.writePage();
	}
}
