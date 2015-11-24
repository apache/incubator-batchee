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
package org.apache.batchee.extras.writer;

import javax.batch.api.chunk.ItemWriter;
import java.io.Serializable;

public abstract class MultiWriterBase implements ItemWriter {
    protected ItemWriter writer1;
    protected ItemWriter writer2;

    protected MultiWriterBase() {
        // no-op
    }

    protected MultiWriterBase(final ItemWriter writer1, final ItemWriter writer2) {
        init(writer1, writer2);
    }

    protected void init(final ItemWriter writer1, final ItemWriter writer2) {
        this.writer1 = writer1;
        this.writer2 = writer2;
    }

    @Override
    public void open(final Serializable serializable) throws Exception {
        if (Checkpoint.class.isInstance(serializable)) {
            final Checkpoint cp = Checkpoint.class.cast(serializable);
            writer1.open(cp.checkpoint1);
            writer2.open(cp.checkpoint2);
        } else if (serializable != null) {
            throw new IllegalArgumentException(serializable + " unsupported");
        } else {
            writer1.open(null);
            writer2.open(null);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            writer1.close();
        } catch (final Exception e) {
            writer2.close();
            throw e;
        }
        writer2.close();
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return new Checkpoint(writer1.checkpointInfo(), writer2.checkpointInfo());
    }

    public static class Checkpoint implements Serializable {
        private Serializable checkpoint1;
        private Serializable checkpoint2;

        public Checkpoint(final Serializable checkpoint1, final Serializable checkpoint2) {
            this.checkpoint1 = checkpoint1;
            this.checkpoint2 = checkpoint2;
        }
    }
}
