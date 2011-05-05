/*
 * Found through Google from:
 *
 * http://research.compaq.com/SRC/WebL/sources/webl.util.BufferedRandomAccessFile.java.html
 *  
 * Copyright status unknown.
 */
package org.acaro.sketches.io;

import java.io.*;
import java.util.Arrays;

/** A <code>BufferedRandomAccessFile</code> is like a
    <code>RandomAccessFile</code>, but it uses a private
    buffer so that most operations do not require a disk
    access.<P>

    Note: The operations on this class are unmonitored.
    Also, the correct functioning of the <code>RandomAccessFile</code>
    methods that are not overridden here relies on the implementation
    of those methods in the superclass.
 */

public final class BufferedRandomAccessFile extends RandomAccessFile {
	static final int LogBuffSz = 16; // 64K buffer
	public static final int BuffSz = (1 << LogBuffSz);
	static final long BuffMask = ~(((long)BuffSz) - 1L);

	private boolean dirty;  // true iff unflushed bytes exist
	private boolean closed; // true iff the file is closed
	public long current;      // current position in file
	private long left, right;    // bounds on characters in "buff"
	private byte[] buff;    // local buffer
	private long valid;     // this.lo + this.buff.length
	private boolean hitEOF; // buffer contains last file block?
	public long diskPos;   // disk position

	/* To describe the above fields, we introduce the following
       abstractions for the file "f":

          len(f)  the length of the file
         curr(f)  the current position in the file
            c(f)  the abstract contents of the file
         disk(f)  the contents of f's backing disk file
       closed(f)  true iff the file is closed

       "curr(f)" is an index in the closed interval [0, len(f)].
       "c(f)" is a character sequence of length "len(f)". "c(f)"
       and "disk(f)" may differ if "c(f)" contains unflushed
       writes not reflected in "disk(f)". The flush operation has
       the effect of making "disk(f)" identical to "c(f)".

       A file is said to be *valid* if the following conditions
       hold:

       V1. The "closed" and "curr" fields are correct:

           f.closed == closed(f)
           f.curr == curr(f)

       V2. The current position is either contained in the buffer,
           or just past the buffer:

           f.left <= f.curr <= f.right

       V3. Any (possibly) unflushed characters are stored
           in "f.buff":

           (forall i in [f.left, f.current):
             c(f)[i] == f.buff[i - f.left])

       V4. For all characters not covered by V3, c(f) and
           disk(f) agree:

           (forall i in [f.left, len(f)):
             i not in [f.left, f.current) => c(f)[i] == disk(f)[i])

       V5. "f.dirty" is true iff the buffer contains bytes that
           should be flushed to the file; by V3 and V4, only part
           of the buffer can be dirty.

           f.dirty ==
           (exists i in [f.left, f.curr):
             c(f)[i] != f.buff[i - f.left])

       V6. this.valid == this.left + this.buff.length

       Note that "f.buff" can be "null" in a valid file, since the
       range of characters in V3 is empty when "f.left == f.current".

       A file is said to be *ready* if the buffer contains the
       current position, i.e., when:

       R1. !f.closed && f.buff != null
           && f.left <= f.current && f.current < f.right
           
       When a file is ready, reading or writing a single byte
       can be performed by reading or writing the in-memory
       buffer without performing a disk operation.
	 */

	/** Open a new <code>BufferedRandomAccessFile</code> on
        <code>file</code> in mode <code>mode</code>, which
        should be "r" for reading only, or "rw" for reading
        and writing. */
	public BufferedRandomAccessFile(File file, String mode)
	throws IOException {
		super(file, mode);
		this.init();
	}

	/** Open a new <code>BufferedRandomAccessFile</code> on the
        file named <code>name</code> in mode <code>mode</code>, 
        which should be "r" for reading only, or "rw" for reading
        and writing. */
	public BufferedRandomAccessFile(String name, String mode)
	throws IOException {
		super(name, mode);
		this.init();
	}

	/* Initialize the private fields of the file so as to
       make it valid. */
	private void init() {
		this.dirty = this.closed = false;
		this.left = this.current = this.right = 0;
		this.buff = new byte[BuffSz];
		this.valid = (long) BuffSz;
		this.hitEOF = false;
		this.diskPos = 0L;
	}

	public void close() throws IOException {
		this.flush();
		this.closed = true;
		super.close();
	}

	/** Flush any bytes in the file's buffer that have not
        yet been written to disk. If the file was created
        read-only, this method is a no-op. */
	public void flush() throws IOException {
		this.flushBuffer();
	}

	/* Flush any dirty bytes in the buffer to disk. */
	private void flushBuffer() throws IOException {
		if (this.dirty) {
			if (this.diskPos != this.left) super.seek(this.left);
			int len = (int)(this.current - this.left);
			super.write(this.buff, 0, len);
			this.diskPos = this.current;
			this.dirty = false;
		}
	}

	/* Read at most "this.buff.length" bytes into "this.buff",
       returning the number of bytes read. If the return result
       is less than "this.buff.length", then EOF was read. */
	private int fillBuffer() throws IOException {
		int cnt = 0;
		int rem = this.buff.length;
		while (rem > 0) {
			int n = super.read(this.buff, cnt, rem); 
			if (n < 0) break;
			cnt += n;
			rem -= n;
		}
		if(this.hitEOF = (cnt < this.buff.length)) {
			// make sure buffer that wasn't read is initialized with -1
			Arrays.fill(this.buff, cnt, this.buff.length, (byte)0xff);
		}
		this.diskPos += cnt;
		return cnt;
	}

	/* This method positions <code>this.curr</code> at position
       <code>pos</code>. If <code>pos</code> does not fall in the
       current buffer, it flushes the current buffer and loads
       the correct one.<p>

       On exit from this routine <code>this.curr == this.hi</code>
       iff <code>pos</code> is at or past the end-of-file, which
       can only happen if the file was opened in read-only mode. */
	public void seek(long pos) throws IOException {
		if (pos >= this.right || pos < this.left) {
			// seeking outside of current buffer -- flush and read
			this.flushBuffer();
			this.left = pos & BuffMask; // start at BuffSz boundary
			this.valid = this.left + (long) this.buff.length;
			if (this.diskPos != this.left) {
				super.seek(this.left); 
				this.diskPos = this.left;
			}
			int n = this.fillBuffer();
			this.right = this.left + (long) n;
		} else {
			// seeking inside current buffer -- no read required
			if (pos < this.current) {
				// if seeking backwards, we must flush to maintain V4
				this.flushBuffer();
			}
		}
		this.current = pos;
	}

	public long getFilePointer() {
		return this.current;
	}

	public long length() throws IOException {
		return Math.max(this.current, super.length());
	}

	public int read() throws IOException {
		if (this.current >= this.right) {
			// test for EOF
			// if (this.hi < this.maxHi) return -1;
			if (this.hitEOF) return -1;

			// slow path -- read another buffer
			this.seek(this.current);
			if (this.current == this.right) return -1;
		}
		byte res = this.buff[(int)(this.current - this.left)];
		this.current++;
		return ((int)res) & 0xFF; // convert byte -> int
	}

	public int read(byte[] b) throws IOException {
		return this.read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		if (this.current >= this.right) {
			// test for EOF
			// if (this.hi < this.maxHi) return -1;
			if (this.hitEOF) return -1;

			// slow path -- read another buffer
			this.seek(this.current);
			if (this.current == this.right) return -1;
		}
		len = Math.min(len, (int)(this.right - this.current));
		int buffOff = (int)(this.current - this.left);
		System.arraycopy(this.buff, buffOff, b, off, len);
		this.current += len;        
		return len;
	}

	public void write(int b) throws IOException {
		if(this.current >= this.right) {
			if (this.hitEOF && this.right < this.valid) {
				// at EOF -- bump "right"
				this.right++;
			} else {
				// slow path -- write current buffer; read next one
				this.seek(this.current);
				if (this.current == this.right) {
					// appending to EOF -- bump "right"
					this.right++;
				}
			}
		}
		this.buff[(int)(this.current - this.left)] = (byte)b;
		this.current++;
		this.dirty = true;
	}

	public void write(byte[] b) throws IOException {
		this.write(b, 0, b.length);
	}

	public void write(byte[] b, int off, int len)
	throws IOException {
		while (len > 0) {
			int wrtn = this.writeAtMost(b, off, len);
			off     += wrtn;
			len     -= wrtn;
		}
		this.dirty = true;
	}

	/* Write at most "len" bytes to "b" starting at position "off",
       and return the number of bytes written. */
	private int writeAtMost(byte[] b, int off, int len)
	throws IOException {
		if (this.current >= this.right) {
			if (this.hitEOF && this.right < this.valid) {
				// at EOF -- bump "right"
				this.right = this.valid;
			} else {
				// slow path -- write current buffer; read next one
				this.seek(this.current);
				if (this.current == this.right) {
					// appending to EOF -- bump "right"
					this.right = this.valid;
				}
			}
		}
		len = Math.min(len, (int)(this.right - this.current));
		int buffOff = (int)(this.current - this.left);
		System.arraycopy(b, off, this.buff, buffOff, len);
		this.current += len;
		return len;
	}
}
