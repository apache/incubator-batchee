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
package org.apache.batchee.csv;

import org.apache.batchee.csv.mapper.DefaultMapper;
import org.apache.batchee.extras.locator.BeanLocator;
import org.apache.batchee.extras.transaction.TransactionalWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.ItemWriter;
import javax.inject.Inject;
import java.io.File;
import java.io.Serializable;
import java.util.List;

public class CommonsCsvWriter implements ItemWriter {
    @Inject
    @BatchProperty
    private String format;

    @Inject
    @BatchProperty
    private String output;

    @Inject
    @BatchProperty
    private String encoding;

    @Inject
    @BatchProperty
    private String mapper;

    @Inject
    @BatchProperty
    private String mapping;

    @Inject
    @BatchProperty
    private String locator;

    @Inject
    @BatchProperty
    private String allowMissingColumnNames;

    @Inject
    @BatchProperty
    private String delimiter;

    @Inject
    @BatchProperty
    private String quoteCharacter;

    @Inject
    @BatchProperty
    private String quoteMode;

    @Inject
    @BatchProperty
    private String commentMarker;

    @Inject
    @BatchProperty
    private String escapeCharacter;

    @Inject
    @BatchProperty
    private String ignoreSurroundingSpaces;

    @Inject
    @BatchProperty
    private String ignoreEmptyLines;

    @Inject
    @BatchProperty
    private String recordSeparator;

    @Inject
    @BatchProperty
    private String nullString;

    @Inject
    @BatchProperty
    private String headerComments;

    @Inject
    @BatchProperty
    private String header;

    @Inject
    @BatchProperty
    private String skipHeaderRecord;

    @Inject
    @BatchProperty
    private String readHeaders;

    private CSVPrinter writer;
    private BeanLocator.LocatorInstance<CsvWriterMapper> mapperInstance;
    private TransactionalWriter transactionalWriter;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final CsvWriterMapper defaultMapper = mapping != null ? new DefaultMapper(Thread.currentThread().getContextClassLoader().loadClass(mapping)) : null;
        mapperInstance = mapper == null ?
            (defaultMapper != null ? new BeanLocator.LocatorInstance<CsvWriterMapper>(defaultMapper, null) : null) :
            BeanLocator.Finder.get(locator).newInstance(CsvWriterMapper.class, mapper);

        if ((header == null || header.isEmpty()) && Boolean.parseBoolean(readHeaders) && DefaultMapper.class.isInstance(mapperInstance.getValue())) {
            header = toListString(DefaultMapper.class.cast(mapperInstance.getValue()).getHeaders());
        }
        final CSVFormat format = newFormat();

        this.transactionalWriter = new TransactionalWriter(new File(output), encoding, checkpoint);
        this.writer = format.print(transactionalWriter);
    }

    private String toListString(final Iterable<String> headers) {
        final StringBuilder b = new StringBuilder();
        for (final String s : headers) {
            b.append(s).append(",");
        }
        if (b.length() > 0) {
            b.setLength(b.length() - 1);
        }
        return b.toString();
    }

    @Override
    public void writeItems(final List<Object> list) throws Exception {
        for (final Object o : list) {
            if (CSVRecord.class.isInstance(o)) {
                writer.printRecord(CSVRecord.class.cast(o));
            } else if (Iterable.class.isInstance(o)) {
                writer.printRecord(Iterable.class.cast(o));
            } else if (mapperInstance != null) {
                writer.printRecord(mapperInstance.getValue().toRecord(o));
            } else {
                throw new IllegalStateException("No way to write " + o + ". Does it implement Iterable<String> or did you set up a mapper?");
            }
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return transactionalWriter.position();
    }

    @Override
    public void close() throws Exception {
        mapperInstance.release();
        if (writer != null) {
            writer.close();
        }
    }

    protected CSVFormat newFormat() {
        return CSVFormatFactory.newFormat(
            format, delimiter, quoteCharacter, quoteMode, commentMarker, escapeCharacter, ignoreSurroundingSpaces,
            ignoreEmptyLines, recordSeparator, nullString, headerComments, header, skipHeaderRecord, allowMissingColumnNames,
            readHeaders);
    }
}
