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
package org.apache.batchee.groovy;

import org.apache.batchee.groovy.util.IOs;
import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class GroovyBatchletTest {
    @Test
    public void read() {
        IOs.write("target/work/batchlet.groovy", "package org.apache.batchee.groovy\n" +
            "\n" +
            "import javax.batch.api.Batchlet\n" +
            "import javax.batch.runtime.context.JobContext\n" +
            "import javax.inject.Inject\n" +
            "\n" +
            "class GBatchlet implements Batchlet {\n" +
            "    @Inject\n" +
            "    JobContext ctx\n" +
            "\n" +
            "    @Override\n" +
            "    String process() throws Exception {\n" +
            "        ctx.setExitStatus(\"G\")\n" +
            "        null\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    void stop() throws Exception {\n" +
            "        //no-op\n" +
            "    }\n" +
            "}\n");
        final JobOperator operator = BatchRuntime.getJobOperator();
        final long id = operator.start("groovy-batchlet", new Properties());
        Batches.waitForEnd(operator, id);
        assertEquals(operator.getJobExecution(id).getExitStatus(), "G");
    }
}
