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
package org.apache.batchee.beanio;

import org.beanio.StreamFactory;

import javax.batch.operations.BatchRuntimeException;
import java.io.InputStream;

public abstract class BeanIOs {
    public static StreamFactory open(final String filePath, final String streamName, final String configuration) throws Exception {
        if (filePath == null) {
            throw new BatchRuntimeException("input can't be null");
        }
        if (streamName == null) {
            throw new BatchRuntimeException("streamName can't be null");
        }
        final StreamFactory streamFactory = StreamFactory.newInstance();
        if (!streamFactory.isMapped(streamName)) {
            final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(configuration);
            if (is == null) {
                throw new BatchRuntimeException("Can't find " + configuration);
            }
            try {
                streamFactory.load(is);
            } finally {
                is.close();
            }
        }
        return streamFactory;
    }
}
