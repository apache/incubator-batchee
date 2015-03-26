/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.cli.jul;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

// taken from tomcat and simplified a bit (no async handler for us)
public class OneLineFormatter extends Formatter {
    private static final String ST_SEP = System.lineSeparator() + " ";
    private static final String TIME_FORMAT = "dd-MMM-yyyy HH:mm:ss";
    private static final ThreadLocal<DateFormat> LOCAL_DATE_CACHE =
            new ThreadLocal<DateFormat>() {
                @Override
                protected DateFormat initialValue() {
                    return new SimpleDateFormat(TIME_FORMAT);
                }
            };

    @Override
    public String format(final LogRecord record) {
        final StringBuilder sb = new StringBuilder();

        // Timestamp
        addTimestamp(sb, record.getMillis());

        // Severity
        sb.append(' ');
        sb.append(record.getLevel());

        // Thread
        sb.append(' ');
        sb.append('[');
        sb.append(Thread.currentThread().getName());
        sb.append(']');

        // Source
        sb.append(' ');
        sb.append(record.getSourceClassName());
        sb.append('.');
        sb.append(record.getSourceMethodName());

        // Message
        sb.append(' ');
        sb.append(formatMessage(record));

        // Stack trace
        if (record.getThrown() != null) {
            sb.append(ST_SEP);
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            record.getThrown().printStackTrace(pw);
            pw.close();
            sb.append(sw.getBuffer());
        }

        sb.append(System.lineSeparator());
        return sb.toString();
    }

    protected void addTimestamp(final StringBuilder buf, final long timestamp) {
        buf.append(LOCAL_DATE_CACHE.get().format(new Date(timestamp)));
        final long frac = timestamp % 1000;
        buf.append('.');
        if (frac < 100) {
            if (frac < 10) {
                buf.append('0');
                buf.append('0');
            } else {
                buf.append('0');
            }
        }
        buf.append(frac);
    }
}
