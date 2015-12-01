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
import org.apache.batchee.doc.api.Documentation;
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

@Documentation("Reads a CSV file using commons-csv.")
public class CommonsCsvReader extends CountedReader {
    private static final CsvReaderMapper<CSVRecord> NOOP_MAPPER = new CsvReaderMapper<CSVRecord>() {
        @Override
        public CSVRecord fromRecord(final CSVRecord record) {
            return record;
        }
    };

    @Inject
    @BatchProperty
    @Documentation("format to use (Default, RFC4180, Excel, TDF, MySQL)")
    private String format;

    @Inject
    @BatchProperty
    @Documentation("file to read")
    private String input;

    @Inject
    @BatchProperty
    @Documentation("input encoding")
    private String encoding;

    @Inject
    @BatchProperty
    @Documentation("record mapper if mapping is null")
    private String mapper;

    @Inject
    @BatchProperty
    @Documentation("mapping type if mapper is null")
    private String mapping;

    @Inject
    @BatchProperty
    @Documentation("locator to lookup the mapper")
    private String locator;

    @Inject
    @BatchProperty
    @Documentation("is missing column names allowed")
    private String allowMissingColumnNames;

    @Inject
    @BatchProperty
    @Documentation("delimiter of the file")
    private String delimiter;

    @Inject
    @BatchProperty
    @Documentation("quote character")
    private String quoteCharacter;

    @Inject
    @BatchProperty
    @Documentation("quote mode (ALL, MINIMAL, NON_NUMERIC, NONE)")
    private String quoteMode;

    @Inject
    @BatchProperty
    @Documentation("comment marker")
    private String commentMarker;

    @Inject
    @BatchProperty
    @Documentation("escape character")
    private String escapeCharacter;

    @Inject
    @BatchProperty
    @Documentation("should the parser ignore surrounding spaces")
    private String ignoreSurroundingSpaces;

    @Inject
    @BatchProperty
    @Documentation("should empty lines be skipped")
    private String ignoreEmptyLines;

    @Inject
    @BatchProperty
    @Documentation("record separator")
    private String recordSeparator;

    @Inject
    @BatchProperty
    @Documentation("string replacement for null")
    private String nullString;

    @Inject
    @BatchProperty
    @Documentation("header comments")
    private String headerComments;

    @Inject
    @BatchProperty
    @Documentation("headers")
    private String header;

    @Inject
    @BatchProperty
    @Documentation("should headers be skipped")
    private String skipHeaderRecord;

    @Inject
    @BatchProperty
    @Documentation("should headers be used")
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

    public void setFormat(final String format) {
        this.format = format;
    }

    public void setFormat(final CSVFormat.Predefined predefined) {
        this.format = predefined.name();
    }

    public void setInput(final String input) {
        this.input = input;
    }

    public void setEncoding(final String encoding) {
        this.encoding = encoding;
    }

    public void setMapper(final String mapper) {
        this.mapper = mapper;
    }

    public void setMapping(final String mapping) {
        this.mapping = mapping;
    }

    public void setLocator(final String locator) {
        this.locator = locator;
    }

    public void setAllowMissingColumnNames(final String allowMissingColumnNames) {
        this.allowMissingColumnNames = allowMissingColumnNames;
    }

    public void setDelimiter(final String delimiter) {
        this.delimiter = delimiter;
    }

    public void setQuoteCharacter(final String quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public void setQuoteMode(final String quoteMode) {
        this.quoteMode = quoteMode;
    }

    public void setCommentMarker(final String commentMarker) {
        this.commentMarker = commentMarker;
    }

    public void setEscapeCharacter(final String escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    public void setIgnoreSurroundingSpaces(final String ignoreSurroundingSpaces) {
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    }

    public void setIgnoreEmptyLines(final String ignoreEmptyLines) {
        this.ignoreEmptyLines = ignoreEmptyLines;
    }

    public void setRecordSeparator(final String recordSeparator) {
        this.recordSeparator = recordSeparator;
    }

    public void setNullString(final String nullString) {
        this.nullString = nullString;
    }

    public void setHeaderComments(final String headerComments) {
        this.headerComments = headerComments;
    }

    public void setHeader(final String header) {
        this.header = header;
    }

    public void setSkipHeaderRecord(final String skipHeaderRecord) {
        this.skipHeaderRecord = skipHeaderRecord;
    }

    public void setReadHeaders(final String readHeaders) {
        this.readHeaders = readHeaders;
    }

    public void readHeaders() {
        this.readHeaders = "true";
    }
}
