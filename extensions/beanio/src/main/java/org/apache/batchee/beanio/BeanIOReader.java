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

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.transaction.CountedReader;
import org.beanio.BeanReader;
import org.beanio.BeanReaderErrorHandler;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Locale;

@Documentation("Uses BeanIO to read a file.")
public class BeanIOReader extends CountedReader implements ItemReader {
    @Inject
    @BatchProperty(name = "file")
    @Documentation("The file to read")
    protected String filePath;

    @Inject
    @BatchProperty(name = "skippedHeaderLines")
    @Documentation("Number of lines to skip")
    protected int skippedHeaderLines;

    @Inject
    @BatchProperty
    @Documentation("The BeanIO stream name")
    protected String streamName;

    @Inject
    @BatchProperty
    @Documentation("The configuration path in the classpath")
    protected String configuration;

    @Inject
    @BatchProperty(name = "locale")
    @Documentation("The locale to use")
    protected String localeStr;

    @Inject
    @BatchProperty(name = "errorHandler")
    @Documentation("The error handler class (resolved using locator)")
    protected String errorHandlerStr;

    private BeanReader reader;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final BufferedReader reader = new BufferedReader(new FileReader(filePath));
        for (int i = 0; i < skippedHeaderLines; i++) {
            reader.readLine();
        }

        this.reader = BeanIOs.open(filePath, streamName, configuration).createReader(streamName, reader, initLocale());
        if (errorHandlerStr != null) {
            final BeanReaderErrorHandler handler = BeanReaderErrorHandler.class.cast(Thread.currentThread().getContextClassLoader().loadClass(errorHandlerStr).newInstance());
            this.reader.setErrorHandler(handler);
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
