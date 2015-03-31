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

import org.apache.batchee.container.jsl.ExecutionElement;
import org.apache.batchee.container.jsl.JobModelResolver;
import org.apache.batchee.container.services.loader.DefaultJobXMLLoaderService;
import org.apache.batchee.jaxb.Batchlet;
import org.apache.batchee.jaxb.Chunk;
import org.apache.batchee.jaxb.End;
import org.apache.batchee.jaxb.Fail;
import org.apache.batchee.jaxb.ItemProcessor;
import org.apache.batchee.jaxb.ItemReader;
import org.apache.batchee.jaxb.ItemWriter;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.JSLProperties;
import org.apache.batchee.jaxb.Next;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.jaxb.Stop;

import java.util.List;

// impl is not a real builder (see immutability etc)
// but this is more to propose a nice and fluent API than anything else
public class StepBuilder {
    public static Step extractFromXml(final String xml, final String name) {
        final JSLJob job = new JobModelResolver().resolveModel(new DefaultJobXMLLoaderService().loadJSL(xml.replace(".xml", "")));
        if (name != null) {
            for (final ExecutionElement step : job.getExecutionElements()) {
                if (Step.class.isInstance(step) && name.equals(Step.class.cast(step).getId())) {
                    return Step.class.cast(step);
                }
            }
        }
        if (job.getExecutionElements().size() == 1) {
            return Step.class.cast(job.getExecutionElements().iterator().next());
        }
        throw new IllegalArgumentException("Step '" + name + "' nor found.");
    }

    public static StepBuilder newStep() {
        return new StepBuilder();
    }

    public static BatchletBuilder newBatchlet() {
        return new StepBuilder().batchlet();
    }

    public static ChunkBuilder newChunk() {
        return new StepBuilder().chunk();
    }

    private StepBuilder() {
        // no-op
    }

    private final Step step = new Step();

    public BatchletBuilder batchlet() {
        final Batchlet batchlet = new Batchlet();
        batchlet.setProperties(new JSLProperties());
        step.setBatchlet(batchlet);
        return new BatchletBuilder(batchlet, this);
    }

    public ChunkBuilder chunk() {
        final Chunk chunk = new Chunk();
        step.setChunk(chunk);
        return new ChunkBuilder(chunk, this);
    }

    public StepBuilder property(final String key, final String value) {
        addProperty(key, value, step.getProperties().getPropertyList());
        return this;
    }

    public StepBuilder name(final String name) {
        step.setId(name);
        return this;
    }

    public StepBuilder failOn(final String on) {
        return failOn(on, null);
    }

    public StepBuilder failOn(final String on, final String exitStatus) {
        final Fail fail = new Fail();
        fail.setOn(on);
        fail.setExitStatus(exitStatus);
        step.getTransitionElements().add(fail);
        return this;
    }

    public StepBuilder endOn(final String on) {
        return endOn(on, null);
    }

    public StepBuilder endOn(final String on, final String exitStatus) {
        final End end = new End();
        end.setOn(on);
        end.setExitStatus(exitStatus);
        step.getTransitionElements().add(end);
        return this;
    }

    public StepBuilder stopOn(final String on, final String restart) {
        return stopOn(on, restart, null);
    }

    public StepBuilder stopOn(final String on, final String restart, final String exitStatus) {
        final Stop stop = new Stop();
        stop.setOn(on);
        stop.setExitStatus(exitStatus);
        stop.setRestart(restart);
        step.getTransitionElements().add(stop);
        return this;
    }

    public StepBuilder nextOn(final String on, final String to) {
        final Next next = new Next();
        next.setOn(on);
        next.setTo(to);
        step.getTransitionElements().add(next);
        return this;
    }

    public Step create() {
        if (step.getId() == null) {
            step.setId("batchee-test"); // can't be null
        }
        return step;
    }

    public static class BatchletBuilder {
        private final Batchlet toBuild;
        private final StepBuilder parent;

        private BatchletBuilder(final Batchlet batchlet, final StepBuilder stepBuilder) {
            toBuild = batchlet;
            parent = stepBuilder;
        }

        public BatchletBuilder ref(final String ref) {
            toBuild.setRef(ref);
            return this;
        }

        public BatchletBuilder property(final String key, final String value) {
            addProperty(key, value, toBuild.getProperties().getPropertyList());
            return this;
        }

        public StepBuilder up() {
            return parent;
        }

        public Step create() {
            return up().create();
        }
    }

    public static class WriterBuilder {
        private final ItemWriter toBuild;
        private final ChunkBuilder parent;

        private WriterBuilder(final ItemWriter batchlet, final ChunkBuilder stepBuilder) {
            toBuild = batchlet;
            parent = stepBuilder;
        }

        public WriterBuilder ref(final String ref) {
            toBuild.setRef(ref);
            return this;
        }

        public WriterBuilder property(final String key, final String value) {
            addProperty(key, value, toBuild.getProperties().getPropertyList());
            return this;
        }

        public ChunkBuilder up() {
            return parent;
        }

        public Step create() {
            return up().up().create();
        }
    }

    public static class ProcessorBuilder {
        private final ItemProcessor toBuild;
        private final ChunkBuilder parent;

        private ProcessorBuilder(final ItemProcessor batchlet, final ChunkBuilder stepBuilder) {
            toBuild = batchlet;
            parent = stepBuilder;
        }

        public ProcessorBuilder ref(final String ref) {
            toBuild.setRef(ref);
            return this;
        }

        public ProcessorBuilder property(final String key, final String value) {
            addProperty(key, value, toBuild.getProperties().getPropertyList());
            return this;
        }

        public WriterBuilder writer() {
            return up().writer();
        }

        public ChunkBuilder up() {
            return parent;
        }
    }

    public static class ReaderBuilder {
        private final ItemReader toBuild;
        private final ChunkBuilder parent;

        private ReaderBuilder(final ItemReader batchlet, final ChunkBuilder stepBuilder) {
            toBuild = batchlet;
            parent = stepBuilder;
        }

        public ReaderBuilder ref(final String ref) {
            toBuild.setRef(ref);
            return this;
        }

        public ReaderBuilder property(final String key, final String value) {
            addProperty(key, value, toBuild.getProperties().getPropertyList());
            return this;
        }

        public ProcessorBuilder processor() {
            return up().processor();
        }

        public WriterBuilder writer() {
            return up().writer();
        }

        public ChunkBuilder up() {
            return parent;
        }
    }

    public static class ChunkBuilder {
        private final StepBuilder parent;
        private final Chunk toBuild;

        private ChunkBuilder(final Chunk chunk, final StepBuilder stepBuilder) {
            toBuild = chunk;
            parent = stepBuilder;
        }

        public ReaderBuilder reader() {
            final ItemReader reader = new ItemReader();
            reader.setProperties(new JSLProperties());
            toBuild.setReader(reader);
            return new ReaderBuilder(reader, this);
        }

        public ProcessorBuilder processor() {
            final ItemProcessor processor = new ItemProcessor();
            processor.setProperties(new JSLProperties());
            toBuild.setProcessor(processor);
            return new ProcessorBuilder(processor, this);
        }

        public WriterBuilder writer() {
            final ItemWriter writer = new ItemWriter();
            writer.setProperties(new JSLProperties());
            toBuild.setWriter(writer);
            return new WriterBuilder(writer, this);
        }

        public ChunkBuilder retryLimit(final int retry) {
            toBuild.setRetryLimit(Integer.toString(retry));
            return this;
        }

        public ChunkBuilder checkpointPolicy(final String policy) {
            toBuild.setCheckpointPolicy(policy);
            return this;
        }

        public StepBuilder up() {
            return parent;
        }
    }

    private static void addProperty(final String key, final String value, final List<Property> propertyList) {
        final Property e = new Property();
        e.setName(key);
        e.setValue(value);
        propertyList.add(e);
    }
}
