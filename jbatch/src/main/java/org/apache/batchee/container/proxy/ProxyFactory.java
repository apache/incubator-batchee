/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.container.proxy;

import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;

import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.spi.BatchArtifactFactory;

import javax.batch.api.Batchlet;
import javax.batch.api.Decider;
import javax.batch.api.chunk.CheckpointAlgorithm;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.api.partition.PartitionAnalyzer;
import javax.batch.api.partition.PartitionCollector;
import javax.batch.api.partition.PartitionMapper;
import javax.batch.api.partition.PartitionReducer;

/*
 * Introduce a level of indirection so proxies are not instantiated directly by newing them up.
 */
public class ProxyFactory {
    static final ThreadLocal<InjectionReferences> INJECTION_CONTEXT = new ThreadLocal<InjectionReferences>();

    private ProxyFactory() {
        // private utility class ct
    }

    protected static Object loadArtifact(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionReferences, final RuntimeJobExecution execution) {
        INJECTION_CONTEXT.set(injectionReferences);
        try {
            final BatchArtifactFactory.Instance instance = factory.load(id);
            if (instance == null) {
                return null;
            }

            if (instance.getReleasable() != null && execution != null) {
                execution.addReleasable(instance.getReleasable());
            }
            return instance.getValue();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            INJECTION_CONTEXT.remove();
        }
    }

    public static InjectionReferences getInjectionReferences() {
        return INJECTION_CONTEXT.get();
    }

    public static <T> T createProxy(T delegate, InjectionReferences injectionRefs, String... nonExceptionHandlingMethods) {
        return (T) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), getInterfaces(delegate.getClass()),
                new BatchProxyInvocationHandler(delegate, injectionRefs, nonExceptionHandlingMethods));

    }


    /*
     * Decider
     */
    public static DeciderProxy createDeciderProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                  final RuntimeJobExecution execution) {
        return new DeciderProxy(Decider.class.cast(loadArtifact(factory, id, injectionRefs, execution)));
    }

    /*
     * Batchlet artifact
     */
    public static Batchlet createBatchletProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                    final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final Batchlet loadedArtifact = (Batchlet) loadArtifact(factory, id, injectionRefs, execution);
        return createProxy(loadedArtifact, injectionRefs);
    }
    
    /*
     * The four main chunk-related artifacts
     */

    public static CheckpointAlgorithmProxy createCheckpointAlgorithmProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                                          final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final CheckpointAlgorithm loadedArtifact = (CheckpointAlgorithm) loadArtifact(factory, id, injectionRefs, execution);
        final CheckpointAlgorithmProxy proxy = new CheckpointAlgorithmProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static ItemReader createItemReaderProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                        final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final ItemReader loadedArtifact = (ItemReader) loadArtifact(factory, id, injectionRefs, execution);
        return createProxy(loadedArtifact, injectionRefs, "readItem");
    }

    public static ItemProcessor createItemProcessorProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                              final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final ItemProcessor loadedArtifact = (ItemProcessor) loadArtifact(factory, id, injectionRefs, execution);
        return createProxy(loadedArtifact, injectionRefs, "processItem");
    }

    public static ItemWriter createItemWriterProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                        final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final ItemWriter loadedArtifact = (ItemWriter) loadArtifact(factory, id, injectionRefs, execution);
        return createProxy(loadedArtifact, injectionRefs, "writeItems");
    }

    /*
     * The four partition-related artifacts
     */

    public static PartitionReducerProxy createPartitionReducerProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                                    final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final PartitionReducer loadedArtifact = (PartitionReducer) loadArtifact(factory, id, injectionRefs, execution);
        final PartitionReducerProxy proxy = new PartitionReducerProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static PartitionMapperProxy createPartitionMapperProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                                  final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final PartitionMapper loadedArtifact = (PartitionMapper) loadArtifact(factory, id, injectionRefs, execution);
        final PartitionMapperProxy proxy = new PartitionMapperProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static PartitionAnalyzerProxy createPartitionAnalyzerProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                                      final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final PartitionAnalyzer loadedArtifact = (PartitionAnalyzer) loadArtifact(factory, id, injectionRefs, execution);
        final PartitionAnalyzerProxy proxy = new PartitionAnalyzerProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    public static PartitionCollectorProxy createPartitionCollectorProxy(final BatchArtifactFactory factory, final String id, final InjectionReferences injectionRefs,
                                                                        final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final PartitionCollector loadedArtifact = (PartitionCollector) loadArtifact(factory, id, injectionRefs, execution);
        final PartitionCollectorProxy proxy = new PartitionCollectorProxy(loadedArtifact);
        proxy.setStepContext(stepContext);
        return proxy;
    }

    /**
     * @return all the interfaces fo the given class and it's superclasses
     */
    private static Class<?>[] getInterfaces(Class<?> clazz) {
        if (clazz.getSuperclass() == Object.class) {
            return clazz.getInterfaces();
        } else {
            Set<Class<?>> clazzes = new HashSet<Class<?>>();
            while (clazz != Object.class) {
                for (Class<?> interfaceClass : clazz.getInterfaces()) {
                    clazzes.add(interfaceClass);
                }
                clazz = clazz.getSuperclass();
            }

            return clazzes.toArray(new Class<?>[clazzes.size()]);
        }
    }
}
