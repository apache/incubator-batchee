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
package org.apache.batchee.extras.flat;

import org.apache.batchee.doc.api.Documentation;
import org.apache.batchee.extras.locator.BeanLocator;
import org.apache.batchee.extras.transaction.CountedReader;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemReader;
import javax.batch.operations.BatchRuntimeException;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;

@Documentation("Reads a flat file.")
public class FlatFileItemReader extends CountedReader implements ItemReader {
    @Inject
    @BatchProperty
    @Documentation("Locator to find line mapper")
    private String locator;

    @Inject
    @BatchProperty
    @Documentation("The line mapper implementation or reference (see locator)")
    private String lineMapper;

    @Inject
    @BatchProperty
    @Documentation("Input file path")
    private String input;

    @Inject
    @BatchProperty(name = "comments")
    @Documentation("Comments prefix marker (default #)")
    private String commentStr;

    private BufferedReader reader = null;
    private String[] comments = new String[0];
    private BeanLocator.LocatorInstance<LineMapper> mapper;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        if (input == null) {
            throw new BatchRuntimeException("Can't find any input");
        }
        final File file = new File(input);
        if (!file.exists()) {
            throw new BatchRuntimeException("'" + input + "' doesn't exist");
        }
        if (lineMapper != null) {
            mapper = BeanLocator.Finder.get(locator).newInstance(LineMapper.class, lineMapper);
        } else {
            mapper = null;
        }

        if (commentStr == null) {
            commentStr = "#";
        }
        comments = commentStr.split(",");

        reader = new BufferedReader(new FileReader(file));
        super.open(checkpoint);
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (mapper != null) {
            mapper.release();
        }
    }

    @Override
    protected Object doRead() throws Exception {
        String line;
        do {
            line = reader.readLine();
            if (line == null) {
                return null;
            }
            incrementReaderCount();
        } while (isComment(line));
        return preReturn(line, items);
    }

    protected boolean isComment(final String line) {
        for (final String prefix : comments) {
            if (line.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    protected Object preReturn(final String rawLine, final long lineNumber) {
        if (mapper != null) {
            return mapper.getValue().map(rawLine, lineNumber);
        }
        return rawLine;
    }
}
