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
package org.apache.batchee.jsefa.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IOs {

    public static final String LINE_SEPARATOR = System.getProperty("line.separator");


    public static void write(final String path, final String content) {
        final File file = new File(path);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new RuntimeException("Can't create " + path);
        }

        try {
            final FileWriter writer = new FileWriter(file);
            writer.write(content);
            writer.close();
        } catch (final IOException e) {
            // no-op
        }
    }

    public static String slurp(final String path) {
        final StringBuilder builder = new StringBuilder();

        for (String line : getLines(path)) {
            builder.append(line).append(LINE_SEPARATOR);
        }

        return builder.toString();
    }

    public static List<String> getLines(final String path) {
        List<String> lines = new ArrayList<String>();

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        catch (Exception e) {
            // no-op
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException e) {
                    // bad luck
                }
            }
        }

        return lines;
    }

    private IOs() {
        // no-op
    }
}
