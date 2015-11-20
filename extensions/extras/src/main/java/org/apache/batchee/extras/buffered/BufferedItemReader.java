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
package org.apache.batchee.extras.buffered;

import org.apache.batchee.extras.typed.NoStateTypedItemReader;

import java.util.Iterator;


/**
 * An ItemReader base class which provides a simple buffering mechanism.
 *
 * The intention is to have a single simple query
 * in {@link #readAllItems()} which first reads
 * all the items and have this class handle the rest.
 *
 * This simple Reader doesn't provide any checkpointInfo
 * ({@link javax.batch.api.chunk.ItemReader#checkpointInfo()})
 * as we only read the items in one go.
 *
 * @param <R> the return type of the reader
 */
public abstract class BufferedItemReader<R> extends NoStateTypedItemReader<R> {

    private IteratorReader<R> valuesIt = null;

    /**
     * This methods need to return all the items to be read.
     * We will 'cache' them and iterate through them until all the
     * items got consumed.
     * @return all the items to be read
     */
    protected abstract Iterator<R> readAllItems();


    @Override
    protected R doRead() {
        if (valuesIt == null) {
            valuesIt = new IteratorReader<R>(readAllItems());
        }
        return valuesIt.read();
    }

}
