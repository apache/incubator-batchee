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

@Documentation("Writes a CSV file using commons-csv.")
public class CommonsCsvWriter implements ItemWriter {
    @Inject
    @BatchProperty
    @Documentation("format to use (Default, RFC4180, Excel, TDF, MySQL)")
    private String format;

    @Inject
    @BatchProperty
    @Documentation("file to write")
    private String output;

    @Inject
    @BatchProperty
    @Documentation("output encoding")
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
    @Documentation("should headers be written")
    private String writeHeaders;

    private CSVPrinter writer;
    private BeanLocator.LocatorInstance<CsvWriterMapper> mapperInstance;
    private TransactionalWriter transactionalWriter;

    @Override
    public void open(final Serializable checkpoint) throws Exception {
        final CsvWriterMapper defaultMapper = mapping != null ? new DefaultMapper(Thread.currentThread().getContextClassLoader().loadClass(mapping)) : null;
        mapperInstance = mapper == null ?
            (defaultMapper != null ? new BeanLocator.LocatorInstance<CsvWriterMapper>(defaultMapper, null) : null) :
            BeanLocator.Finder.get(locator).newInstance(CsvWriterMapper.class, mapper);

        if ((header == null || header.isEmpty()) && Boolean.parseBoolean(writeHeaders) && DefaultMapper.class.isInstance(mapperInstance.getValue()) && checkpoint == null) {
            header = toListString(DefaultMapper.class.cast(mapperInstance.getValue()).getHeaders());
        }
        final CSVFormat format = newFormat();

        final File file = new File(output);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new IllegalStateException("Cant create " + file);
        }
        this.transactionalWriter = new TransactionalWriter(file, encoding, checkpoint);
        this.writer = format.print(transactionalWriter);
    }

    private String toListString(final Iterable<String> headers) {
        final StringBuilder b = new StringBuilder();
        for (final String s : headers) {
            final boolean quote = s.contains(",");
            if (quote) {
                b.append('"');
            }
            b.append(s);
            if (quote) {
                b.append('"');
            }
            b.append(",");
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
        writer.flush();
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
            writeHeaders);
    }

    public void setFormat(final String format) {
        this.format = format;
    }

    public void setFormat(final CSVFormat.Predefined predefined) {
        this.format = predefined.name();
    }

    public void setOutput(final String output) {
        this.output = output;
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

    public void setWriteHeaders(final String readHeaders) {
        this.writeHeaders = readHeaders;
    }

    public void writeHeaders() {
        this.writeHeaders = "true";
    }
}
