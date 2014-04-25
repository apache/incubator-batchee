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
package org.apache.batchee.container.proxy;

import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.services.ServicesManager;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Listener;
import org.apache.batchee.jaxb.Listeners;
import org.apache.batchee.jaxb.Property;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.spi.BatchArtifactFactory;


import javax.batch.api.listener.JobListener;
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
                jobLevelListenerInfo.add(createListener(factory, listener, injectionRefs, execution));
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

                list.add(createListener(factory, listener, injectionRefs, execution));
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
                            stepListenerInfoList.add(createListener(factory, listener, injectionRefs, execution));
                        }
                    }

                    stepListenerInfoList.addAll(globalListeners(factory, "org.apache.batchee.step.listeners.after", injectionRefs, execution));
                }
            }
        }
        return stepListenerInfoList;
    }

    private static ListenerInfo createListener(final BatchArtifactFactory factory, final Listener listener, final InjectionReferences injectionRefs,
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
            if (JobListener.class.isAssignableFrom(li.getArtifact().getClass())) {
                retVal.add(ProxyFactory.createProxy((JobListener) li.getArtifact(), injectionRefs));
            }
        }
        return retVal;
    }

    public <T> List<T> getListeners(Class<T> listenerClazz, final Step step, final InjectionReferences injectionRefs,
                                    final RuntimeJobExecution execution) {
        final List<ListenerInfo> stepListenerInfo = getStepListenerInfo(factory, step, injectionRefs, execution);
        final List<T> retVal = new ArrayList<T>();
        for (final ListenerInfo li : stepListenerInfo) {
            if (listenerClazz.isAssignableFrom(li.getArtifact().getClass())) {
                final T proxy = ProxyFactory.createProxy((T) li.getArtifact(), injectionRefs);
                retVal.add(proxy);
            }
        }

        return retVal;
    }

    private static class ListenerInfo {
        private Object listenerArtifact = null;

        private ListenerInfo(final Object listenerArtifact) {
            this.listenerArtifact = listenerArtifact;
        }

        Object getArtifact() {
            return listenerArtifact;
        }
    }

}
