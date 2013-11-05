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

import org.apache.batchee.extras.transaction.CountedReader;
import org.beanio.BeanReader;
import org.beanio.BeanReaderErrorHandler;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.inject.Inject;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Locale;

public class BeanIOReader extends CountedReader implements ItemReader {
    @Inject
    @BatchProperty(name = "file")
    protected String filePath;

    @Inject
    @BatchProperty
    protected String streamName;

    @Inject
    @BatchProperty
    protected String configuration;

    @Inject
    @BatchProperty(name = "locale")
    protected String localeStr;

    @Inject
    @BatchProperty(name = "errorHandler")
    protected String errorHandlerStr;

    private BeanReader reader;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        reader = BeanIOs.open(filePath, streamName, configuration).createReader(streamName, new FileReader(filePath), initLocale());
        if (errorHandlerStr != null) {
            final BeanReaderErrorHandler handler = BeanReaderErrorHandler.class.cast(Thread.currentThread().getContextClassLoader().loadClass(errorHandlerStr).newInstance());
            reader.setErrorHandler(handler);
        }

        super.open(checkpoint);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    @Override
    protected Object doRead() throws Exception {
        return reader.read();
    }

    private Locale initLocale() {
        if (localeStr == null) {
            return Locale.getDefault();
        }

        final String[] s = localeStr.split("_");
        if (s.length >= 3) {
            return new Locale(s[0], s[1], s[2]);
        } else if (s.length == 2) {
            return new Locale(s[0], s[1]);
        }
        return new Locale(localeStr);
    }
}
