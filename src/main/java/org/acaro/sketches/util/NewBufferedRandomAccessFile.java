/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acaro.sketches.util;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;


public class NewBufferedRandomAccessFile extends RandomAccessFile {
    // absolute filesystem path to the file
    private final String filePath;

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65535;

    // isDirty - true if this.buffer contains any un-synced bytes
    private boolean isDirty, syncNeeded;

    // buffer which will cache file blocks
    private byte[] buffer;

    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid; this will be LESS than buffer capacity if buffer is not full!
    private long bufferOffset, current = 0;
    private int validBufferBytes = 0;

    // constant, used for caching purpose, -1 if file is open in "rw" mode
    // otherwise this will hold cached file length
    private final long fileLength;

    // channel liked with the file, used to retrieve data and force updates.
    private final FileChannel channel;

    private long minBufferOffset = Long.MAX_VALUE;

    public NewBufferedRandomAccessFile(String name, String mode) throws IOException
    {
        this(new File(name), mode, DEFAULT_BUFFER_SIZE);
    }

    public NewBufferedRandomAccessFile(String name, String mode, int bufferSize) throws IOException
    {
        this(new File(name), mode, bufferSize);
    }

    public NewBufferedRandomAccessFile(File file, String mode) throws IOException
    {
        this(file, mode, DEFAULT_BUFFER_SIZE);
    }

    public NewBufferedRandomAccessFile(File file, String mode, int bufferSize) throws IOException
    {
        super(file, mode);

        channel = super.getChannel();
        filePath = file.getAbsolutePath();

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        buffer = new byte[bufferSize];
        reBuffer();

        // if in read-only mode, caching file size
        fileLength = (mode.equals("r")) ? this.channel.size() : -1;
    }

    public void sync() throws IOException
    {
        if (syncNeeded)
        {
            flush();
            channel.force(true); // true, because file length counts as
            syncNeeded = false;
        }
    }

    public void flush() throws IOException
    {
        if (isDirty)
        {
            if (channel.position() != bufferOffset)
                channel.position(bufferOffset);

            super.write(buffer, 0, validBufferBytes);

            isDirty = false;
        }
    }

    @Override
    public void setLength(long newLength) throws IOException
    {
        if (newLength < 0)
            throw new IllegalArgumentException();

        // account for dirty data in buffers
        if (isDirty)
        {
            if (newLength < bufferOffset)
            {
                // buffer is garbage
                validBufferBytes = 0;
            }
            else if (newLength > (bufferOffset + validBufferBytes))
            {
                // flush everything in buffer
                flush();
            }
            else // buffer within range
            {
                // truncate buffer and flush
                validBufferBytes = (int)(newLength - bufferOffset);
                flush();
            }
        }

        // at this point all dirty buffer data is flushed
        super.setLength(newLength);

        validBufferBytes = 0;
        current = newLength;
        reBuffer();
    }

    private void reBuffer() throws IOException
    {
        flush(); // synchronizing buffer and file on disk

        bufferOffset = current;
        if (bufferOffset >= channel.size())
        {
            validBufferBytes = 0;
            return;
        }

        if (bufferOffset < minBufferOffset)
            minBufferOffset = bufferOffset;

        channel.position(bufferOffset); // setting channel position
        int read = 0;
        while (read < buffer.length)
        {
            int n = super.read(buffer, read, buffer.length - read);
            if (n < 0)
                break;
            read += n;
        }
        validBufferBytes = read;
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read() throws IOException
    {
        if (isEOF())
            return -1; // required by RandomAccessFile

        if (current >= bufferOffset + buffer.length)
            reBuffer();
        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xFF;
    }

    @Override
    public int read(byte[] buffer) throws IOException
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length) throws IOException
    {
        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (current >= bufferOffset + buffer.length)
            reBuffer();
        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        int toCopy = Math.min(length, validBufferBytes - (int) (current - bufferOffset));
        System.arraycopy(buffer, (int) (current - bufferOffset), buff, offset, toCopy);
        current += toCopy;

        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws IOException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;

        byte[] buff = new byte[length];
        readFully(buff); // reading data buffer

        return ByteBuffer.wrap(buff);
    }

    private final byte[] singleByteBuffer = new byte[1]; // so we can use the write(byte[]) path w/o tons of new byte[] allocations
    @Override
    public void write(int val) throws IOException
    {
        singleByteBuffer[0] = (byte) val;
        this.write(singleByteBuffer, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] buff, int offset, int length) throws IOException
    {
        if (buffer == null)
            throw new ClosedChannelException();

        if (isReadOnly())
            throw new IOException("Unable to write: file is in the read-only mode.");

        while (length > 0)
        {
            int n = writeAtMost(buff, offset, length);
            offset += n;
            length -= n;
            isDirty = true;
            syncNeeded = true;
        }
    }

    private boolean isReadOnly()
    {
        return fileLength != -1;
    }

    /*
     * Write at most "length" bytes from "b" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(byte[] buff, int offset, int length) throws IOException
    {
        if (current >= bufferOffset + buffer.length)
            reBuffer();
        assert current < bufferOffset + buffer.length;

        int positionWithinBuffer = (int) (current - bufferOffset);
        int toCopy = Math.min(length, buffer.length - positionWithinBuffer);
        System.arraycopy(buff, offset, buffer, positionWithinBuffer, toCopy);
        current += toCopy;
        validBufferBytes = Math.max(validBufferBytes, positionWithinBuffer + toCopy);
        assert current <= bufferOffset + buffer.length;

        return toCopy;
    }

    @Override
    public void seek(long newPosition) throws IOException
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (isReadOnly() && newPosition > fileLength)
            throw new EOFException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                 newPosition, filePath, fileLength));

        current = newPosition;

        if (newPosition >= bufferOffset + validBufferBytes || newPosition < bufferOffset)
            reBuffer(); // this will set bufferEnd for us
    }

    @Override
    public int skipBytes(int count) throws IOException
    {
        if (count > 0)
        {
            long currentPos = getFilePointer(), eof = length();
            int newCount = (int) ((currentPos + count > eof) ? eof - currentPos : count);

            seek(currentPos + newCount);
            return newCount;
        }

        return 0;
    }

    public long length() throws IOException
    {
        return (fileLength == -1) ? Math.max(Math.max(current, channel.size()), bufferOffset + validBufferBytes) : fileLength;
    }

    public long getFilePointer()
    {
        return current;
    }

    public String getPath()
    {
        return filePath;
    }

    public boolean isEOF() throws IOException
    {
        return getFilePointer() == length();
    }

    @Override
    public void close() throws IOException
    {
        sync();
        buffer = null;

        super.close();
    }
}