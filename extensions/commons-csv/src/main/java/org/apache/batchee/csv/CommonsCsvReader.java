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
import org.apache.batchee.extras.buffered.IteratorReader;
import org.apache.batchee.extras.locator.BeanLocator;
import org.apache.batchee.extras.transaction.CountedReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import javax.batch.api.BatchProperty;
import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

public class CommonsCsvReader extends CountedReader {
    private static final CsvReaderMapper<CSVRecord> NOOP_MAPPER = new CsvReaderMapper<CSVRecord>() {
        @Override
        public CSVRecord fromRecord(final CSVRecord record) {
            return record;
        }
    };

    @Inject
    @BatchProperty
    private String format;

    @Inject
    @BatchProperty
    private String input;

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

    private IteratorReader<CSVRecord> iterator;
    private CSVParser parser;
    private BeanLocator.LocatorInstance<CsvReaderMapper> mapperInstance;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final CSVFormat csvFormat = newFormat();
        parser = csvFormat.parse(newReader());
        iterator = new IteratorReader<CSVRecord>(parser.iterator());

        mapperInstance = mapper == null ?
            new BeanLocator.LocatorInstance<CsvReaderMapper>(
                mapping != null ? new DefaultMapper(Thread.currentThread().getContextClassLoader().loadClass(mapping)) : NOOP_MAPPER, null) :
            BeanLocator.Finder.get(locator).newInstance(CsvReaderMapper.class, mapper);


        super.open(checkpoint);
    }

    @Override
    protected Object doRead() throws Exception {
        final CSVRecord read = iterator.read();
        return read != null ? mapperInstance.getValue().fromRecord(read) : null;
    }

    @Override
    public void close() throws Exception {
        mapperInstance.release();
        if (parser != null) {
            parser.close();
        }
    }

    protected Reader newReader() {
        try { // no need of BufferedReader since [csv] does it
            return encoding != null ? new InputStreamReader(new FileInputStream(input), encoding) : new FileReader(input);
        } catch (final FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        } catch (final UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected CSVFormat newFormat() {
        return CSVFormatFactory.newFormat(
            format, delimiter, quoteCharacter, quoteMode, commentMarker, escapeCharacter, ignoreSurroundingSpaces,
            ignoreEmptyLines, recordSeparator, nullString, headerComments, header, skipHeaderRecord, allowMissingColumnNames,
            readHeaders);
    }
}
