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
package org.apache.batchee.container.navigator;

import org.apache.batchee.container.jsl.ExecutionElement;
import org.apache.batchee.container.jsl.GlobPatternMatcherImpl;
import org.apache.batchee.container.jsl.IllegalTransitionException;
import org.apache.batchee.container.jsl.Transition;
import org.apache.batchee.container.jsl.TransitionElement;
import org.apache.batchee.container.status.ExecutionStatus;
import org.apache.batchee.container.status.ExtendedBatchStatus;
import org.apache.batchee.jaxb.Decision;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.Next;
import org.apache.batchee.jaxb.Split;
import org.apache.batchee.jaxb.Step;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractNavigator<T> implements ModelNavigator<T> {
    private Map<String, ExecutionElement> alreadyExecutedElements = new HashMap<String, ExecutionElement>();

    public ExecutionElement getFirstExecutionElement(List<ExecutionElement> peerExecutionElements, String restartOn) throws IllegalTransitionException {
        ExecutionElement startElement;

        if (restartOn != null) {
            startElement = getExecutionElementFromId(peerExecutionElements, restartOn);
            if (startElement == null) {
                throw new IllegalStateException("Didn't find an execution element maching restart-on designated element: " + restartOn);
            }
        } else {
            if (peerExecutionElements.size() > 0) {
                startElement = peerExecutionElements.get(0);
            } else {
                return null;
            }
        }

        // We allow repeating a decision
        if (!(startElement instanceof Decision)) {
            alreadyExecutedElements.put(startElement.getId(), startElement);
        }

        validateElementType(startElement);

        return startElement;
    }


    /**
     * Precedence is: look at elements, then look at attribute, then return quietly
     *
     * @param currentElem
     * @param peerExecutionElements
     * @param currentStatus
     * @return
     * @throws IllegalTransitionException
     */
    public Transition getNextTransition(ExecutionElement currentElem, List<ExecutionElement> peerExecutionElements, ExecutionStatus currentStatus)
        throws IllegalTransitionException {
        Transition returnTransition = new Transition();

        ExecutionElement nextExecutionElement = null;

        List<TransitionElement> transitionElements = currentElem.getTransitionElements();

        // Check the transition elements first.
        if (!transitionElements.isEmpty()) {
            for (final TransitionElement t : transitionElements) {
                boolean isMatched = matchExitStatusAgainstOnAttribute(currentStatus.getExitStatus(), t);
                if (isMatched) {
                    if (t instanceof Next) {
                        Next next = (Next) t;
                        nextExecutionElement = getExecutionElementFromId(peerExecutionElements, next.getTo());
                        returnTransition.setNextExecutionElement(nextExecutionElement);
                        break;
                    } else {
                        returnTransition.setTransitionElement(t);
                    }
                    return returnTransition;
                }
            }
        }

        // We've returned already if we matched a Stop, End or Fail
        if (nextExecutionElement == null) {
            if (currentStatus.getExtendedBatchStatus().equals(ExtendedBatchStatus.EXCEPTION_THROWN)) {
                returnTransition.setNoTransitionElementMatchAfterException();
                return returnTransition;
            } else {
                nextExecutionElement = getNextExecutionElemFromAttribute(peerExecutionElements, currentElem);
                returnTransition.setNextExecutionElement(nextExecutionElement);
            }
        }

        if (nextExecutionElement != null) {
            if (alreadyExecutedElements.containsKey(nextExecutionElement.getId())) {
                throw new IllegalTransitionException("Execution loop detected !!!  Trying to re-execute execution element: " + nextExecutionElement.getId());
            }

            // We allow repeating a decision
            if (!(nextExecutionElement instanceof Decision)) {
                alreadyExecutedElements.put(nextExecutionElement.getId(), nextExecutionElement);
            }
        } else {
            returnTransition.setFinishedTransitioning();
        }
        return returnTransition;
    }


    private ExecutionElement getExecutionElementFromId(List<ExecutionElement> executionElements, String id)
        throws IllegalTransitionException {
        if (id != null) {
            for (final ExecutionElement elem : executionElements) {
                if (elem.getId().equals(id)) {
                    validateElementType(elem);
                    return elem;
                }
            }
            throw new IllegalTransitionException("No execution element found with id = " + id);
        }
        return null;
    }

    private static boolean matchSpecifiedExitStatus(final String currentStepExitStatus, final String exitStatusPattern) {
        return new GlobPatternMatcherImpl().matchWithoutBackslashEscape(currentStepExitStatus, exitStatusPattern);
    }

    private boolean matchExitStatusAgainstOnAttribute(final String exitStatus, final TransitionElement elem) {
        return matchSpecifiedExitStatus(exitStatus, elem.getOn());
    }

    private ExecutionElement getNextExecutionElemFromAttribute(List<ExecutionElement> peerExecutionElements, ExecutionElement currentElem) throws IllegalTransitionException {
        ExecutionElement nextExecutionElement = null;
        String nextAttrId;
        if (currentElem instanceof Step) {
            nextAttrId = ((Step) currentElem).getNextFromAttribute();
            nextExecutionElement = getExecutionElementFromId(peerExecutionElements, nextAttrId);
        } else if (currentElem instanceof Split) {
            nextAttrId = ((Split) currentElem).getNextFromAttribute();
            nextExecutionElement = getExecutionElementFromId(peerExecutionElements, nextAttrId);
        } else if (currentElem instanceof Flow) {
            nextAttrId = ((Flow) currentElem).getNextFromAttribute();
            nextExecutionElement = getExecutionElementFromId(peerExecutionElements, nextAttrId);
        } else if (currentElem instanceof Decision) {
            // Nothing special to do in this case.
        }

        validateElementType(nextExecutionElement);

        return nextExecutionElement;
    }

    private void validateElementType(ExecutionElement elem) {
        if (elem != null) {
            if (!((elem instanceof Decision) || (elem instanceof Flow) || (elem instanceof Split) || (elem instanceof Step))) {
                throw new IllegalArgumentException("Unknown execution element found, elem = " + elem + ", found with type: " + elem.getClass().getCanonicalName() +
                    " , which is not an instance of Decision, Flow, Split, or Step.");
            }
        }
    }

}
