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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class CSVFormatFactory {
    private CSVFormatFactory() {
        // no-op
    }
//CHECKSTYLE:OFF
    static CSVFormat newFormat(final String format,
                                      final String delimiter,
                                      final String quoteCharacter,
                                      final String quoteMode,
                                      final String commentMarker,
                                      final String escapeCharacter,
                                      final String ignoreSurroundingSpaces,
                                      final String ignoreEmptyLines,
                                      final String recordSeparator,
                                      final String nullString,
                                      final String headerComments,
                                      final String header,
                                      final String skipHeaderRecord,
                                      final String allowMissingColumnNames,
                                      final String readHeaders) {
//CHECKSTYLE:ON
        CSVFormat out = format == null ? CSVFormat.DEFAULT : CSVFormat.valueOf(format);
        if (delimiter != null) {
            out = out.withDelimiter(delimiter.charAt(0));
        }
        if (quoteCharacter != null) {
            out = out.withQuote(quoteCharacter.charAt(0));
        }
        if (quoteMode != null) {
            out = out.withQuoteMode(QuoteMode.valueOf(quoteMode));
        }
        if (commentMarker != null) {
            out = out.withCommentMarker(commentMarker.charAt(0));
        }
        if (escapeCharacter != null) {
            out = out.withEscape(escapeCharacter.charAt(0));
        }
        if (ignoreSurroundingSpaces != null) {
            out = out.withIgnoreSurroundingSpaces(Boolean.parseBoolean(ignoreSurroundingSpaces));
        }
        if (ignoreEmptyLines != null) {
            out = out.withIgnoreEmptyLines(Boolean.parseBoolean(ignoreEmptyLines));
        }
        if (recordSeparator != null) {
            if ("\\n".equals(recordSeparator)) {
                out = out.withRecordSeparator('\n');
            } else if ("\\r\\n".equals(recordSeparator)) {
                out = out.withRecordSeparator("\r\n");
            } else {
                out = out.withRecordSeparator(recordSeparator);
            }
        }
        if (nullString != null) {
            out = out.withNullString(nullString);
        }
        if (headerComments != null && !headerComments.trim().isEmpty()) {
            out = out.withHeaderComments(headerComments.split(" *, *"));
        }
        if (Boolean.parseBoolean(readHeaders)) {
            out = out.withHeader();
        }
        if (header != null && !header.trim().isEmpty()) {
            try { // headers can have CSV header names so parse it there
                final Iterator<CSVRecord> iterator = out.withHeader(new String[0]).parse(new StringReader(header + '\n' + header)).iterator();
                final CSVRecord record = iterator.next();
                final List<String> list = new ArrayList<String>(record.size());
                for (final String h : record) {
                    list.add(h);
                }
                out = out.withHeader(list.toArray(new String[record.size()]));
            } catch (final IOException e) { // can't occur actually
                out = out.withHeader(header.split(" *, *"));
            }
        }
        if (skipHeaderRecord != null) {
            out = out.withSkipHeaderRecord(Boolean.parseBoolean(skipHeaderRecord));
        }
        if (allowMissingColumnNames != null) {
            out = out.withAllowMissingColumnNames(Boolean.parseBoolean(allowMissingColumnNames));
        }
        return out;
    }
}
