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
package org.apache.batchee.test;

import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import javax.batch.api.BatchProperty;
import javax.batch.api.Batchlet;
import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.StepExecution;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class StepLauncherTest {

    public static final String BATCHLET_REF = SimpleBatchlet.class.getName();

    @Test
    public void stepFail() {
        final JobExecution execution = StepLauncher.exec(StepBuilder.extractFromXml("sleep.xml", "doSleep"), new Properties() {{
            setProperty("exitStatus", "failed");
        }}).jobExecution();
        assertEquals(execution.getExitStatus(), "oops");
        assertEquals(execution.getBatchStatus(), BatchStatus.FAILED);
    }

    @Test
    public void stepFromXml() {
        final StepExecution execution = StepLauncher.execute(StepBuilder.extractFromXml("sleep.xml", "doSleep"));
        assertEquals("OK", execution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
    }

    @Test
    public void simpleBatchlet() {
        final StepExecution execution = StepLauncher.execute(StepBuilder.newBatchlet().ref(BATCHLET_REF).create());
        assertEquals("default", execution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
    }

    @Test
    public void batchletWithConfig() {
        final StepExecution execution = StepLauncher.execute(
                StepBuilder.newBatchlet()
                        .ref(BATCHLET_REF)
                        .property("config", "override")
                        .create());
        assertEquals("override", execution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
    }

    @Test
    public void batchletWithConfigFromJobParams() {
        final StepExecution execution = StepLauncher.execute(
                StepBuilder.newBatchlet()
                        .ref(BATCHLET_REF)
                        .property("config", "#{jobParameters['conf']}")
                        .create(),
                new Properties() {{
                    setProperty("conf", "param");
                }});
        assertEquals("param", execution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
    }

    @Test
    public void simpleChunk() {
        final StepExecution execution = StepLauncher.execute(StepBuilder.newChunk()
                .reader().ref(SimpleReader.class.getName())
                .processor().ref(SimpleProcessor.class.getName())
                .writer().ref(SimpleWriter.class.getName())
                .create());
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
        assertNotNull(SimpleWriter.result);
        assertEquals(2, SimpleWriter.result.size());
        assertTrue(SimpleWriter.result.contains("0#"));
        assertTrue(SimpleWriter.result.contains("1#"));
    }

    @Test
    public void configuredReader() {
        final StepExecution execution = StepLauncher.execute(StepBuilder.newChunk()
                .reader().ref(SimpleReader.class.getName()).property("total", "1")
                .writer().ref(SimpleWriter.class.getName())
                .create());
        assertEquals(BatchStatus.COMPLETED, execution.getBatchStatus());
        assertNotNull(SimpleWriter.result);
        assertEquals(1, SimpleWriter.result.size());
        assertTrue(SimpleWriter.result.contains("#0"));
    }

    public static class SimpleReader extends AbstractItemReader {
        @Inject
        @BatchProperty
        private String total;

        private int count = 2;

        @Override
        public void open(final Serializable checkpoint) throws Exception {
            if (total != null) {
                count = Integer.parseInt(total);
            }
        }

        @Override
        public Object readItem() throws Exception {
            if (count-- > 0) {
                return "#" + count;
            }
            return null;
        }
    }

    public static class SimpleProcessor implements ItemProcessor {
        @Override
        public Object processItem(final Object o) throws Exception {
            return new StringBuilder(o.toString()).reverse().toString();
        }
    }

    public static class SimpleWriter extends AbstractItemWriter {
        public static List<Object> result;

        @Override
        public void writeItems(final List<Object> objects) throws Exception {
            result = objects;
        }
    }

    public static class SimpleBatchlet implements Batchlet {
        @Inject
        private JobContext job;

        @Inject
        private StepContext step;

        @Inject
        @BatchProperty
        private String config;

        @Override
        public String process() throws Exception {
            if (config != null) {
                job.setExitStatus(config);
                step.setExitStatus(config);
                return config;
            }

            step.setExitStatus("default");
            return "default";
        }

        @Override
        public void stop() throws Exception {
            // no-op
        }
    }
}
