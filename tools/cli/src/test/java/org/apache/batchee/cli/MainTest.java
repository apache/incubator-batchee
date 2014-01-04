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
package org.apache.batchee.cli;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.StandardOutputStreamLog;

import static org.apache.batchee.cli.BatchEECLI.main;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class MainTest {
    @Rule
    public final StandardOutputStreamLog stdout = new StandardOutputStreamLog();

    @Test
    public void start() {
        main(new String[]{ "start", "-name", "sample" });
        assertThat(stdout.getLog(), containsString("Batch 'sample' started with id #"));
        assertThat(stdout.getLog(), containsString("Batch status: COMPLETED"));
    }

    @Test
    public void startAndFail() {
        main(new String[]{ "start", "-name", "sample", "status=fail" });
        assertThat(stdout.getLog(), containsString("Batch 'sample' started with id #"));
        assertThat(stdout.getLog(), containsString("Batch status: FAILED"));
    }

    @Test
    public void restart() {
        main(new String[]{ "start", "-name", "sample", "status=fail" });
        final String str = "Batch 'sample' started with id #";

        assertThat(stdout.getLog(), containsString(str));
        final int idx = stdout.getLog().indexOf(str);
        final int end = stdout.getLog().indexOf(System.getProperty("line.separator"), idx);
        final String id = stdout.getLog().substring(idx + str.length(), end);

        main(new String[]{ "restart", "-id", id, "status=ok" });
        assertThat(stdout.getLog(), containsString(str));
        assertThat(stdout.getLog(), containsString("Batch status: COMPLETED"));
    }
}
