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
package org.apache.batchee.camel;

import org.apache.batchee.doc.api.Documentation;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.inject.Inject;
import java.io.Serializable;

@Documentation("Uses a camel consumer as reader.")
public class CamelItemReader implements ItemReader {
    @Inject
    @BatchProperty
    @Documentation("Endpoint URI")
    private String endpoint;

    @Inject
    @BatchProperty(name = "timeout")
    @Documentation("Timeout after which we consider the consumption is done")
    private String timeoutStr;

    @Inject
    @BatchProperty
    @Documentation("Consumer stream type")
    private String expectedClass;

    @Inject
    @BatchProperty
    @Documentation("Locator for the consumer template")
    private String templateLocator;

    @Override
    public Object readItem() throws Exception {
        final Class<?> expected;
        if (expectedClass != null) {
            expected = Thread.currentThread().getContextClassLoader().loadClass(expectedClass);
        } else {
            expected = null;
        }

        final long timeout;
        if (timeoutStr == null) {
            timeout = 1000;
        } else {
            timeout = Integer.parseInt(timeoutStr);
        }

        return CamelBridge.receive(templateLocator, endpoint, timeout, expected);
    }

    @Override
    public void close() throws Exception {
        // nothing
    }

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        // no-op: supportable?
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null; // supportable?
    }
}
