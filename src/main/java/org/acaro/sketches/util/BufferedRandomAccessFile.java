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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A <code>BufferedRandomAccessFile</code> is like a
 * <code>RandomAccessFile</code>, but it uses a private buffer so that most
 * operations do not require a disk access.
 * <P>
 * 
 * Note: The operations on this class are unmonitored. Also, the correct
 * functioning of the <code>RandomAccessFile</code> methods that are not
 * overridden here relies on the implementation of those methods in the
 * superclass.
 */
public class BufferedRandomAccessFile extends RandomAccessFile {
    // absolute filesystem path to the file
    private final String filePath;

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65535;

    // isDirty - true if this.buffer contains any un-synced bytes
    // hitEOF - true if buffer capacity is less then it's maximal size
    private boolean isDirty, syncNeeded, hitEOF = false;

    // buffer which will cache file blocks
    private ByteBuffer buffer;

    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `bufferEnd` is `bufferOffset` + count of bytes read from file
    private long bufferOffset, bufferEnd, current = 0;

    // max buffer size is set according to (int size) parameter in the
    // constructor
    // or in directIO() method to the DEFAULT_DIRECT_BUFFER_SIZE
    private long maxBufferSize;

    // constant, used for caching purpose, -1 if file is open in "rw" mode
    // otherwise this will hold cached file length
    private final long fileLength;

    // channel liked with the file, used to retrieve data and force updates.
    private final FileChannel channel;

    /*
     * Open a new <code>BufferedRandomAccessFile</code> on the file named
     * <code>name</code> in mode <code>mode</code>, which should be "r" for
     * reading only, or "rw" for reading and writing.
     */
    public BufferedRandomAccessFile(String name, String mode) throws IOException {
        this(new File(name), mode, 0);
    }

    public BufferedRandomAccessFile(String name, String mode, int bufferSize) throws IOException {
        this(new File(name), mode, bufferSize);
    }

    /*
     * Open a new <code>BufferedRandomAccessFile</code> on <code>file</code> in
     * mode <code>mode</code>, which should be "r" for reading only, or "rw" for
     * reading and writing.
     */
    public BufferedRandomAccessFile(File file, String mode) throws IOException {
        this(file, mode, 0);
    }

    public BufferedRandomAccessFile(File file, String mode, int bufferSize) throws IOException {
        this(file, mode, bufferSize, false);
    }

    public BufferedRandomAccessFile(File file, String mode, int bufferSize, boolean skipCache) throws IOException {
        super(file, mode);

        channel = super.getChannel();
        filePath = file.getAbsolutePath();

        maxBufferSize = Math.max(bufferSize, DEFAULT_BUFFER_SIZE);

        // allocating required size of the buffer
        buffer = ByteBuffer.allocate((int) maxBufferSize);

        // if in read-only mode, caching file size
        fileLength = (mode.equals("r")) ? this.channel.size() : -1;
        bufferEnd = reBuffer(); // bufferBottom equals to the bytes read
    }

    public void sync() throws IOException {
        if (syncNeeded) {
            flush();

            channel.force(true); // true, because file length counts as
                                 // "meta-data"
            syncNeeded = false;
        }
    }

    public void flush() throws IOException {
        if (isDirty) {
            if (channel.position() != bufferOffset)
                channel.position(bufferOffset);

            int lengthToWrite = (int) (bufferEnd - bufferOffset);

            super.write(buffer.array(), 0, lengthToWrite);

            isDirty = false;
        }
    }

    private long reBuffer() throws IOException {
        flush(); // synchronizing buffer and file on disk
        buffer.clear();
        bufferOffset = current;

        if (bufferOffset > channel.size()) {
            buffer.rewind();
            bufferEnd = bufferOffset;
            hitEOF = true;

            return 0;
        }

        channel.position(bufferOffset); // setting channel position
        long bytesRead = channel.read(buffer); // reading from that position

        hitEOF = (bytesRead < maxBufferSize); // buffer is not fully loaded with
                                              // data
        bufferEnd = bufferOffset + bytesRead;

        buffer.rewind();

        return bytesRead;
    }

    @Override
    // -1 will be returned if EOF is reached, RandomAccessFile is responsible
    // for
    // throwing EOFException
    public int read() throws IOException {
        if (isEOF())
            return -1; // required by RandomAccessFile

        if (current < bufferOffset || current >= bufferEnd) {
            reBuffer();

            if (current == bufferEnd && hitEOF)
                return -1; // required by RandomAccessFile
        }

        byte result = buffer.get();
        current++;

        return ((int) result) & 0xFF;
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if EOF is reached, RandomAccessFile is responsible
    // for
    // throwing EOFException
    public int read(byte[] buff, int offset, int length) throws IOException {
        int bytesCount = 0;

        while (length > 0) {
            int bytesRead = readAtMost(buff, offset, length);
            if (bytesRead == -1)
                return -1; // EOF

            offset += bytesRead;
            length -= bytesRead;
            bytesCount += bytesRead;
        }

        return bytesCount;
    }

    private int readAtMost(byte[] buff, int offset, int length) throws IOException {
        if (length >= bufferEnd && hitEOF)
            return -1;

        final int left = (int) maxBufferSize - buffer.position();
        if (current < bufferOffset || left < length) {
            reBuffer();
        }

        length = Math.min(length, (int) (maxBufferSize - buffer.position()));
        buffer.get(buff, offset, length);
        current += length;

        return length;
    }

    public ByteBuffer readBytes(int length) throws IOException {
        assert length >= 0 : "buffer length should not be negative: " + length;

        byte[] buff = new byte[length];
        readFully(buff); // reading data buffer

        return ByteBuffer.wrap(buff);
    }

    @Override
    public void write(int val) throws IOException {
        byte[] b = new byte[1];
        b[0] = (byte) val;
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] buff, int offset, int length) throws IOException {
        while (length > 0) {
            int n = writeAtMost(buff, offset, length);
            offset += n;
            length -= n;
            isDirty = true;
            syncNeeded = true;
        }
    }

    /*
     * Write at most "length" bytes from "b" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(byte[] buff, int offset, int length) throws IOException {
        final int left = (int) maxBufferSize - buffer.position();
        if (current < bufferOffset || left < length) {
            reBuffer();
        }

        // logic is the following: we need to add bytes to the end of the buffer
        // starting from current buffer position and return this length
        length = Math.min(length, (int) (maxBufferSize - buffer.position()));

        buffer.put(buff, offset, length);
        current += length;

        if (current > bufferEnd)
            bufferEnd = current;

        return length;
    }

    @Override
    public void seek(long newPosition) throws IOException {
        current = newPosition;

        if (newPosition >= bufferEnd || newPosition < bufferOffset) {
            reBuffer(); // this will set bufferEnd for us
        }

        final int delta = (int) (newPosition - bufferOffset);
        buffer.position(delta);
    }

    @Override
    public int skipBytes(int count) throws IOException {
        if (count > 0) {
            long currentPos = getFilePointer(), eof = length();
            int newCount = (int) ((currentPos + count > eof) ? eof - currentPos : count);

            seek(currentPos + newCount);
            return newCount;
        }

        return 0;
    }

    public long length() throws IOException {
        return (fileLength == -1) ? Math.max(current, channel.size()) : fileLength;
    }

    public long getFilePointer() {
        return bufferOffset + buffer.position();
    }

    public String getPath() {
        return filePath;
    }

    public boolean isEOF() throws IOException {
        return getFilePointer() == length();
    }

    public long bytesRemaining() throws IOException {
        return length() - getFilePointer();
    }

    @Override
    public void close() throws IOException {
        sync();
        buffer = null;

        super.close();
    }
}
