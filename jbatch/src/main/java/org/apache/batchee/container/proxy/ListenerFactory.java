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

import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Listener;
import org.apache.batchee.jaxb.Listeners;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.BatchArtifactFactory;

import javax.batch.api.chunk.listener.ChunkListener;
import javax.batch.api.chunk.listener.ItemProcessListener;
import javax.batch.api.chunk.listener.ItemReadListener;
import javax.batch.api.chunk.listener.ItemWriteListener;
import javax.batch.api.chunk.listener.RetryProcessListener;
import javax.batch.api.chunk.listener.RetryReadListener;
import javax.batch.api.chunk.listener.RetryWriteListener;
import javax.batch.api.chunk.listener.SkipProcessListener;
import javax.batch.api.chunk.listener.SkipReadListener;
import javax.batch.api.chunk.listener.SkipWriteListener;
import javax.batch.api.listener.JobListener;
import javax.batch.api.listener.StepListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ListenerFactory {
    private final BatchArtifactFactory factory;
    private final List<ListenerInfo> jobLevelListenerInfo;
    private final Map<String, List<ListenerInfo>> stepLevelListenerInfo = new ConcurrentHashMap<String, List<ListenerInfo>>();

    /*
     * Build job-level ListenerInfo(s) up-front, but build step-level ones
     * lazily.
     */
    public ListenerFactory(final BatchArtifactFactory factory, final JSLJob jobModel, final InjectionReferences injectionRefs, final RuntimeJobExecution execution) {
        jobLevelListenerInfo = new ArrayList<ListenerInfo>();
        this.factory = factory;

        Listeners jobLevelListeners = jobModel.getListeners();

        jobLevelListenerInfo.addAll(globalListeners(factory, "org.apache.batchee.job.listeners.before", injectionRefs, execution));
        if (jobLevelListeners != null) {
            for (final Listener listener : jobLevelListeners.getListenerList()) {
                jobLevelListenerInfo.add(buildListenerInfo(factory, listener, injectionRefs, execution));
            }
        }
        jobLevelListenerInfo.addAll(globalListeners(factory, "org.apache.batchee.job.listeners.after", injectionRefs, execution));
    }

    private static Collection<ListenerInfo> globalListeners(final BatchArtifactFactory factory, final String key, final InjectionReferences injectionRefs,
                                                            final RuntimeJobExecution execution) {
        final String globalListeners = ServicesManager.value(key, null);
        if (globalListeners != null) {
            final String[] refs = globalListeners.split(",");

            final Collection<ListenerInfo> list = new ArrayList<ListenerInfo>(refs.length);
            for (final String ref : refs) {
                final Listener listener = new Listener();
                listener.setRef(ref);

                list.add(buildListenerInfo(factory, listener, injectionRefs, execution));
            }

            return list;
        }
        return Collections.emptyList();
    }

    /*
     * Does NOT throw an exception if a step-level listener is annotated with
     * 
     * @JobListener, even if that is the only type of listener annotation found.
     */
    private List<ListenerInfo> getStepListenerInfo(final BatchArtifactFactory factory, final Step step, final InjectionReferences injectionRefs,
                                                   final RuntimeJobExecution execution) {
        List<ListenerInfo> stepListenerInfoList = stepLevelListenerInfo.get(step.getId());
        if (stepListenerInfoList == null) {
            synchronized (this) {
                stepListenerInfoList = stepLevelListenerInfo.get(step.getId());
                if (stepListenerInfoList == null) {
                    stepListenerInfoList = new ArrayList<ListenerInfo>();
                    stepLevelListenerInfo.put(step.getId(), stepListenerInfoList);

                    stepListenerInfoList.addAll(globalListeners(factory, "org.apache.batchee.step.listeners.before", injectionRefs, execution));

                    final Listeners stepLevelListeners = step.getListeners();
                    if (stepLevelListeners != null) {
                        for (final Listener listener : stepLevelListeners.getListenerList()) {
                            stepListenerInfoList.add(buildListenerInfo(factory, listener, injectionRefs, execution));
                        }
                    }

                    stepListenerInfoList.addAll(globalListeners(factory, "org.apache.batchee.step.listeners.after", injectionRefs, execution));
                }
            }
        }
        return stepListenerInfoList;
    }

    private static ListenerInfo buildListenerInfo(final BatchArtifactFactory factory, final Listener listener, final InjectionReferences injectionRefs,
                                                  final RuntimeJobExecution execution) {
        final String id = listener.getRef();
        final List<Property> propList = (listener.getProperties() == null) ? null : listener.getProperties().getPropertyList();

        injectionRefs.setProps(propList);
        final Object listenerArtifact = ProxyFactory.loadArtifact(factory, id, injectionRefs, execution);
        if (listenerArtifact == null) {
            throw new IllegalArgumentException("Load of artifact id: " + id + " returned <null>.");
        }
        return new ListenerInfo(listenerArtifact);

    }

    public List<JobListener> getJobListeners(final InjectionReferences injectionRefs) {
        final List<JobListener> retVal = new ArrayList<JobListener>();
        for (final ListenerInfo li : jobLevelListenerInfo) {
            if (li.isJobListener()) {
                retVal.add(ProxyFactory.createProxy((JobListener) li.getArtifact(), injectionRefs));
            }
        }
        return retVal;
    }

    public List<ChunkListener> getChunkListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                      final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<ChunkListener> retVal = new ArrayList<ChunkListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isChunkListener()) {
                final ChunkListener proxy = ProxyFactory.createProxy((ChunkListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<ItemProcessListener> getItemProcessListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                                  final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<ItemProcessListener> retVal = new ArrayList<ItemProcessListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isItemProcessListener()) {
                ItemProcessListener proxy = ProxyFactory.createProxy((ItemProcessListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<ItemReadListener> getItemReadListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                            final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<ItemReadListener> retVal = new ArrayList<ItemReadListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isItemReadListener()) {
                final ItemReadListener proxy = ProxyFactory.createProxy((ItemReadListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<ItemWriteListener> getItemWriteListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                              final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<ItemWriteListener> retVal = new ArrayList<ItemWriteListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isItemWriteListener()) {
                final ItemWriteListener proxy = ProxyFactory.createProxy((ItemWriteListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<RetryProcessListener> getRetryProcessListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                                    final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<RetryProcessListener> retVal = new ArrayList<RetryProcessListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isRetryProcessListener()) {
                final RetryProcessListener proxy = ProxyFactory.createProxy((RetryProcessListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<RetryReadListener> getRetryReadListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                              final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<RetryReadListener> retVal = new ArrayList<RetryReadListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isRetryReadListener()) {
                final RetryReadListener proxy = ProxyFactory.createProxy((RetryReadListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<RetryWriteListener> getRetryWriteListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                                final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<RetryWriteListener> retVal = new ArrayList<RetryWriteListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isRetryWriteListener()) {
                final RetryWriteListener proxy = ProxyFactory.createProxy((RetryWriteListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<SkipProcessListener> getSkipProcessListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                                  final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<SkipProcessListener> retVal = new ArrayList<SkipProcessListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isSkipProcessListener()) {
                final SkipProcessListener proxy = ProxyFactory.createProxy((SkipProcessListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<SkipReadListener> getSkipReadListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                            final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<SkipReadListener> retVal = new ArrayList<SkipReadListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isSkipReadListener()) {
                final SkipReadListener proxy = ProxyFactory.createProxy((SkipReadListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<SkipWriteListener> getSkipWriteListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                              final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<SkipWriteListener> retVal = new ArrayList<SkipWriteListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isSkipWriteListener()) {
                final SkipWriteListener proxy = ProxyFactory.createProxy((SkipWriteListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<StepListener> getStepListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext,
                                                    final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<StepListener> retVal = new ArrayList<StepListener>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isStepListener()) {
                final StepListener proxy = ProxyFactory.createProxy((StepListener) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    //X TODO way too complicated it seems...
    private static class ListenerInfo {
        private Object listenerArtifact = null;
        private Class listenerArtifactClass = null;

        private ListenerInfo(final Object listenerArtifact) {
            this.listenerArtifact = listenerArtifact;
            this.listenerArtifactClass = listenerArtifact.getClass();
        }

        Object getArtifact() {
            return listenerArtifact;
        }

        boolean isJobListener() {
            return JobListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isStepListener() {
            return StepListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isChunkListener() {
            return ChunkListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isItemProcessListener() {
            return ItemProcessListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isItemReadListener() {
            return ItemReadListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isItemWriteListener() {
            return ItemWriteListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isRetryReadListener() {
            return RetryReadListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isRetryWriteListener() {
            return RetryWriteListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isRetryProcessListener() {
            return RetryProcessListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isSkipProcessListener() {
            return SkipProcessListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isSkipReadListener() {
            return SkipReadListener.class.isAssignableFrom(listenerArtifactClass);
        }

        boolean isSkipWriteListener() {
            return SkipWriteListener.class.isAssignableFrom(listenerArtifactClass);
        }
    }

}
