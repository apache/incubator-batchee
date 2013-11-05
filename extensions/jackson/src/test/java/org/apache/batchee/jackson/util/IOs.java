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
package org.apache.batchee.jackson.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class IOs {
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
        try {
            final BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            do {
                line = reader.readLine();
                if (line != null) {
                    builder.append(line);
                }
            } while (line != null);
            reader.close();
        } catch (final Exception e) {
            // no-op
        }
        return builder.toString();
    }

    private IOs() {
        // no-op
    }
}
