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

import java.util.Iterator;

/**
 * A shortcut class to use an Iterator as delegation in a reader. Allow to not mandate the inheritance with BufferedItemReader.
 *
 * @param <E> the expected Iterator item type.
 */
public class IteratorReader<E> {
    private final Iterator<E> delegate;

    public IteratorReader(final Iterator<E> delegate) {
        this.delegate = delegate;
    }

    public E read() {
        return delegate != null && delegate.hasNext() ? delegate.next() : null;
    }
}
