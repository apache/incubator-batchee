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
    private List<ListenerInfo> jobLevelListenerInfo = null;

    private Map<String, List<ListenerInfo>> stepLevelListenerInfo = new ConcurrentHashMap<String, List<ListenerInfo>>();

    /*
     * Build job-level ListenerInfo(s) up-front, but build step-level ones
     * lazily.
     */
    public ListenerFactory(final JSLJob jobModel, final InjectionReferences injectionRefs, final RuntimeJobExecution execution) {
        jobLevelListenerInfo = new ArrayList<ListenerInfo>();

        Listeners jobLevelListeners = jobModel.getListeners();

        jobLevelListenerInfo.addAll(globalListeners("org.apache.batchee.job.listeners.before", injectionRefs, execution));
        if (jobLevelListeners != null) {
            for (final Listener listener : jobLevelListeners.getListenerList()) {
                jobLevelListenerInfo.add(buildListenerInfo(listener, injectionRefs, execution));
            }
        }
        jobLevelListenerInfo.addAll(globalListeners("org.apache.batchee.job.listeners.after", injectionRefs, execution));
    }

    private static Collection<ListenerInfo> globalListeners(final String key, final InjectionReferences injectionRefs, final RuntimeJobExecution execution) {
        final String globalListeners = ServicesManager.value(key, null);
        if (globalListeners != null) {
            final String[] refs = globalListeners.split(",");

            final Collection<ListenerInfo> list = new ArrayList<ListenerInfo>(refs.length);
            for (final String ref : refs) {
                final Listener listener = new Listener();
                listener.setRef(ref);

                list.add(buildListenerInfo(listener, injectionRefs, execution));
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
    private List<ListenerInfo> getStepListenerInfo(final Step step, final InjectionReferences injectionRefs, final RuntimeJobExecution execution) {
        List<ListenerInfo> stepListenerInfoList = stepLevelListenerInfo.get(step.getId());
        if (stepListenerInfoList == null) {
            synchronized (this) {
                stepListenerInfoList = stepLevelListenerInfo.get(step.getId());
                if (stepListenerInfoList == null) {
                    stepListenerInfoList = new ArrayList<ListenerInfo>();
                    stepLevelListenerInfo.put(step.getId(), stepListenerInfoList);

                    stepListenerInfoList.addAll(globalListeners("org.apache.batchee.step.listeners.before", injectionRefs, execution));

                    final Listeners stepLevelListeners = step.getListeners();
                    if (stepLevelListeners != null) {
                        for (final Listener listener : stepLevelListeners.getListenerList()) {
                            stepListenerInfoList.add(buildListenerInfo(listener, injectionRefs, execution));
                        }
                    }

                    stepListenerInfoList.addAll(globalListeners("org.apache.batchee.step.listeners.after", injectionRefs, execution));
                }
            }
        }
        return stepListenerInfoList;
    }

    private static ListenerInfo buildListenerInfo(final Listener listener, final InjectionReferences injectionRefs, final RuntimeJobExecution execution) {
        final String id = listener.getRef();
        final List<Property> propList = (listener.getProperties() == null) ? null : listener.getProperties().getPropertyList();

        injectionRefs.setProps(propList);
        final Object listenerArtifact = ProxyFactory.loadArtifact(id, injectionRefs, execution);
        if (listenerArtifact == null) {
            throw new IllegalArgumentException("Load of artifact id: " + id + " returned <null>.");
        }
        return new ListenerInfo(listenerArtifact);

    }

    public List<JobListenerProxy> getJobListeners() {
        final List<JobListenerProxy> retVal = new ArrayList<JobListenerProxy>();
        for (final ListenerInfo li : jobLevelListenerInfo) {
            if (li.isJobListener()) {
                retVal.add(new JobListenerProxy((JobListener) li.getArtifact()));
            }
        }
        return retVal;
    }

    public List<ChunkListenerProxy> getChunkListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<ChunkListenerProxy> retVal = new ArrayList<ChunkListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isChunkListener()) {
                final ChunkListenerProxy proxy = new ChunkListenerProxy((ChunkListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<ItemProcessListenerProxy> getItemProcessListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<ItemProcessListenerProxy> retVal = new ArrayList<ItemProcessListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isItemProcessListener()) {
                ItemProcessListenerProxy proxy = new ItemProcessListenerProxy((ItemProcessListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<ItemReadListenerProxy> getItemReadListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<ItemReadListenerProxy> retVal = new ArrayList<ItemReadListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isItemReadListener()) {
                final ItemReadListenerProxy proxy = new ItemReadListenerProxy((ItemReadListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<ItemWriteListenerProxy> getItemWriteListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<ItemWriteListenerProxy> retVal = new ArrayList<ItemWriteListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isItemWriteListener()) {
                final ItemWriteListenerProxy proxy = new ItemWriteListenerProxy((ItemWriteListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<RetryProcessListenerProxy> getRetryProcessListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<RetryProcessListenerProxy> retVal = new ArrayList<RetryProcessListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isRetryProcessListener()) {
                final RetryProcessListenerProxy proxy = new RetryProcessListenerProxy((RetryProcessListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<RetryReadListenerProxy> getRetryReadListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<RetryReadListenerProxy> retVal = new ArrayList<RetryReadListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isRetryReadListener()) {
                final RetryReadListenerProxy proxy = new RetryReadListenerProxy((RetryReadListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<RetryWriteListenerProxy> getRetryWriteListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<RetryWriteListenerProxy> retVal = new ArrayList<RetryWriteListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isRetryWriteListener()) {
                final RetryWriteListenerProxy proxy = new RetryWriteListenerProxy((RetryWriteListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<SkipProcessListenerProxy> getSkipProcessListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<SkipProcessListenerProxy> retVal = new ArrayList<SkipProcessListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isSkipProcessListener()) {
                final SkipProcessListenerProxy proxy = new SkipProcessListenerProxy((SkipProcessListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<SkipReadListenerProxy> getSkipReadListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<SkipReadListenerProxy> retVal = new ArrayList<SkipReadListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isSkipReadListener()) {
                final SkipReadListenerProxy proxy = new SkipReadListenerProxy((SkipReadListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<SkipWriteListenerProxy> getSkipWriteListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<SkipWriteListenerProxy> retVal = new ArrayList<SkipWriteListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isSkipWriteListener()) {
                final SkipWriteListenerProxy proxy = new SkipWriteListenerProxy((SkipWriteListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    public List<StepListenerProxy> getStepListeners(final Step step, final InjectionReferences injectionRefs, final StepContextImpl stepContext, final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(step, injectionRefs, execution);
        final List<StepListenerProxy> retVal = new ArrayList<StepListenerProxy>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (li.isStepListener()) {
                final StepListenerProxy proxy = new StepListenerProxy((StepListener) li.getArtifact());
                proxy.setStepContext(stepContext);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

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
