/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.extras.transaction;

import org.apache.batchee.extras.transaction.integration.SynchronizationService;
import org.apache.batchee.extras.transaction.integration.Synchronizations;

import javax.batch.operations.BatchRuntimeException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TransactionalWriter extends Writer {
    private static final String BASE_BUFFER_KEY = TransactionalWriter.class.getName() + ".buffer";

    private final String encoding;
    private final String bufferKey;
    private final FileChannel delegate;
    private long position = 0;
    private boolean closed = false;

    public TransactionalWriter(final File file, final String encoding, final Serializable checkpoint) throws FileNotFoundException {
        this.delegate = new RandomAccessFile(file, "rw").getChannel();
        this.bufferKey = BASE_BUFFER_KEY + "." + hashCode();
        if (encoding != null) {
            this.encoding = encoding;
        } else {
            this.encoding = "UTF-8";
        }

        final long restartOffset;
        if (checkpoint != null && Number.class.isInstance(checkpoint)) {
            restartOffset = Number.class.cast(checkpoint).longValue();
            try {
                if (delegate.size() < restartOffset) {
                    throw new BatchRuntimeException("File seems too small");
                }
            } catch (final IOException e) {
                throw new BatchRuntimeException(e);
            }
        } else {
            restartOffset = 0;
        }
        try {
            delegate.truncate(restartOffset);
            delegate.position(restartOffset);
        } catch (final IOException e) {
            throw new BatchRuntimeException(e);
        }
        position = restartOffset;
    }

    @Override
    public void write(final char[] cbuf, final int off, final int len) throws IOException {
        if (Synchronizations.hasTransaction()) {
            buffer().append(cbuf, off, len);
        } else {
            final String string = String.valueOf(cbuf, off, off + len);
            if(delegate.write(ByteBuffer.wrap(string.getBytes(encoding))) != string.length()) {
                throw new IOException("Some data were not written");
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (!Synchronizations.hasTransaction()) {
            fileFlush();
        } else {
            position = delegate.position() + buffer().length();
        }
    }

    private void fileFlush() throws IOException {
        delegate.force(false);
        position = delegate.position();
    }

    @Override
    public void close() throws IOException {
        if ((!Synchronizations.hasTransaction() || Synchronizations.get(bufferKey) == null) && delegate.isOpen()) {
            fileFlush();
            delegate.close();
        } else {
            closed = true;
        }
    }

    public long position() {
        return position;
    }

    private StringBuilder buffer() {
        StringBuilder buf = StringBuilder.class.cast(Synchronizations.get(bufferKey));
        if (buf == null) {
            final StringBuilder buffer = new StringBuilder();
            buf = buffer;
            Synchronizations.put(bufferKey, buffer);
            Synchronizations.registerSynchronization(new SynchronizationService.OnCommit() {
                @Override
                public void afterCommit() {
                    try {
                        final byte[] bytes = buffer.toString().getBytes(encoding);
                        if(delegate.write(ByteBuffer.wrap(bytes)) != bytes.length) {
                            throw new BatchRuntimeException("Some part of the chunk was not written");
                        }
                        delegate.force(false);
                    } catch (final IOException ioe) {
                        throw new BatchRuntimeException(ioe);
                    }
                    close();
                }

                @Override
                public void afterRollback() {
                    close();
                }

                private void close() {
                    if (closed && delegate.isOpen()) {
                        try {
                            delegate.close();
                        } catch (final IOException e) {
                            throw new BatchRuntimeException(e);
                        }
                    }
                }
            });
        }
        return buf;
    }
}
