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
package org.apache.batchee.container.impl.controller;

import org.apache.batchee.container.Controller;
import org.apache.batchee.container.ExecutionElementController;
import org.apache.batchee.container.exception.BatchContainerRuntimeException;
import org.apache.batchee.container.impl.JobContextImpl;
import org.apache.batchee.container.impl.StepContextImpl;
import org.apache.batchee.container.impl.jobinstance.RuntimeJobExecution;
import org.apache.batchee.container.jsl.ExecutionElement;
import org.apache.batchee.container.jsl.IllegalTransitionException;
import org.apache.batchee.container.jsl.Transition;
import org.apache.batchee.container.jsl.TransitionElement;
import org.apache.batchee.container.navigator.ModelNavigator;
import org.apache.batchee.container.status.ExecutionStatus;
import org.apache.batchee.container.status.ExtendedBatchStatus;
import org.apache.batchee.container.util.PartitionDataWrapper;
import org.apache.batchee.jaxb.Decision;
import org.apache.batchee.jaxb.End;
import org.apache.batchee.jaxb.Fail;
import org.apache.batchee.jaxb.Flow;
import org.apache.batchee.jaxb.JSLJob;
import org.apache.batchee.jaxb.Split;
import org.apache.batchee.jaxb.Step;
import org.apache.batchee.jaxb.Stop;

import javax.batch.runtime.BatchStatus;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ExecutionTransitioner {
    private RuntimeJobExecution jobExecution;
    private long rootJobExecutionId;
    private ModelNavigator<?> modelNavigator;

    // 'volatile' since it receives stop on separate thread.
    private volatile ExecutionElementController currentStoppableElementController;
    private ExecutionElementController previousElementController;
    private ExecutionElement currentExecutionElement = null;
    private ExecutionElement previousExecutionElement = null;


    private JobContextImpl jobContext;
    private BlockingQueue<PartitionDataWrapper> analyzerQueue = null;

    private List<Long> stepExecIds;

    public ExecutionTransitioner(RuntimeJobExecution jobExecution, long rootJobExecutionId, ModelNavigator<?> modelNavigator) {
        this.jobExecution = jobExecution;
        this.rootJobExecutionId = rootJobExecutionId;
        this.modelNavigator = modelNavigator;
        this.jobContext = jobExecution.getJobContext();
    }

    public ExecutionTransitioner(RuntimeJobExecution jobExecution, long rootJobExecutionId, ModelNavigator<JSLJob> jobNavigator, BlockingQueue<PartitionDataWrapper> analyzerQueue) {
        this.jobExecution = jobExecution;
        this.rootJobExecutionId = rootJobExecutionId;
        this.modelNavigator = jobNavigator;
        this.jobContext = jobExecution.getJobContext();
        this.analyzerQueue = analyzerQueue;
    }

    public ExecutionStatus doExecutionLoop() {
        try {
            currentExecutionElement = modelNavigator.getFirstExecutionElement(jobExecution.getRestartOn());
        } catch (final IllegalTransitionException e) {
            throw new IllegalArgumentException("Could not transition to first execution element within job.", e);
        }

        while (true) {

            if (jobContext.getBatchStatus().equals(BatchStatus.STOPPING)) {
                return new ExecutionStatus(ExtendedBatchStatus.JOB_OPERATOR_STOPPING);
            }

            final ExecutionElementController currentElementController = getNextElementController();
            currentStoppableElementController = currentElementController;

            final ExecutionStatus status = currentElementController.execute();

            // Nothing special for decision or step except to get exit status.  For flow and split we want to bubble up though.
            if ((currentExecutionElement instanceof Split) || (currentExecutionElement instanceof Flow)) {
                // Exit status and restartOn should both be in the job context.
                if (!status.getExtendedBatchStatus().equals(ExtendedBatchStatus.NORMAL_COMPLETION)) {
                    return status;
                }
            }

            // Seems like this should only happen if an Error is thrown at the step level, since normally a step-level
            // exception is caught and the fact that it was thrown capture in the ExecutionStatus
            if (jobContext.getBatchStatus().equals(BatchStatus.FAILED)) {
                throw new BatchContainerRuntimeException("Sub-execution returned its own BatchStatus of FAILED.  Deal with this by throwing exception to the next layer.");
            }

            // set the execution element controller to null so we don't try to call stop on it after the element has finished executing
            currentStoppableElementController = null;

            if (jobContext.getBatchStatus().equals(BatchStatus.STOPPING)) {
                return new ExecutionStatus(ExtendedBatchStatus.JOB_OPERATOR_STOPPING);
            }

            Transition nextTransition;
            try {
                nextTransition = modelNavigator.getNextTransition(currentExecutionElement, status);
            } catch (final IllegalTransitionException e) {
                throw new BatchContainerRuntimeException("Problem transitioning to next execution element.", e);
            }

            //
            // We will find ourselves in one of four states now.
            //
            // 1. Finished transitioning after a normal execution, but nothing to do 'next'.
            // 2. We just executed a step which through an exception, but didn't match a transition element.
            // 3. We are going to 'next' to another execution element (and jump back to the top of this '
            //    'while'-loop.
            // 4. We matched a terminating transition element (<end>, <stop> or <fail).
            //

            // 1.
            if (nextTransition.isFinishedTransitioning()) {
                this.stepExecIds = currentElementController.getLastRunStepExecutions();
                // Consider just passing the last 'status' back, but let's unwrap the exit status and pass a new NORMAL_COMPLETION
                // status back instead.
                return new ExecutionStatus(ExtendedBatchStatus.NORMAL_COMPLETION, status.getExitStatus());
                // 2.
            } else if (nextTransition.noTransitionElementMatchedAfterException()) {
                return new ExecutionStatus(ExtendedBatchStatus.EXCEPTION_THROWN, status.getExitStatus());
                // 3.
            } else if (nextTransition.getNextExecutionElement() != null) {
                // hold on to the previous execution element for the decider
                // we need it because we need to inject the context of the
                // previous execution element into the decider
                previousExecutionElement = currentExecutionElement;
                previousElementController = currentElementController;
                currentExecutionElement = nextTransition.getNextExecutionElement();
                // 4.
            } else if (nextTransition.getTransitionElement() != null) {
                return handleTerminatingTransitionElement(nextTransition.getTransitionElement());
            } else {
                throw new IllegalStateException("Not sure how we'd end up in this state...aborting rather than looping.");
            }
        }
    }


    private ExecutionElementController getNextElementController() {
        final ExecutionElementController elementController;

        if (currentExecutionElement instanceof Decision) {
            final Decision decision = (Decision) currentExecutionElement;
            elementController = ExecutionElementControllerFactory.getDecisionController(jobExecution, decision);
            final DecisionController decisionController = (DecisionController) elementController;
            decisionController.setPreviousStepExecutions(previousExecutionElement, previousElementController);
        } else if (currentExecutionElement instanceof Flow) {
            final Flow flow = (Flow) currentExecutionElement;
            elementController = ExecutionElementControllerFactory.getFlowController(jobExecution, flow, rootJobExecutionId);
        } else if (currentExecutionElement instanceof Split) {
            final Split split = (Split) currentExecutionElement;
            elementController = ExecutionElementControllerFactory.getSplitController(jobExecution, split, rootJobExecutionId);
        } else if (currentExecutionElement instanceof Step) {
            final Step step = (Step) currentExecutionElement;
            final StepContextImpl stepContext = new StepContextImpl(step.getId());
            elementController = ExecutionElementControllerFactory.getStepController(jobExecution, step, stepContext, rootJobExecutionId, analyzerQueue);
        } else {
            elementController = null;
        }
        return elementController;
    }


    private ExecutionStatus handleTerminatingTransitionElement(final TransitionElement transitionElement) {
        ExecutionStatus retVal;
        if (transitionElement instanceof Stop) {

            Stop stopElement = (Stop) transitionElement;
            String restartOn = stopElement.getRestart();
            String exitStatusFromJSL = stopElement.getExitStatus();

            retVal = new ExecutionStatus(ExtendedBatchStatus.JSL_STOP);

            if (exitStatusFromJSL != null) {
                jobContext.setExitStatus(exitStatusFromJSL);
                retVal.setExitStatus(exitStatusFromJSL);
            }
            if (restartOn != null) {
                jobContext.setRestartOn(restartOn);
                retVal.setRestartOn(restartOn);
            }
        } else if (transitionElement instanceof End) {
            final End endElement = (End) transitionElement;
            final String exitStatusFromJSL = endElement.getExitStatus();
            retVal = new ExecutionStatus(ExtendedBatchStatus.JSL_END);
            if (exitStatusFromJSL != null) {
                jobContext.setExitStatus(exitStatusFromJSL);
                retVal.setExitStatus(exitStatusFromJSL);
            }
        } else if (transitionElement instanceof Fail) {
            final Fail failElement = (Fail) transitionElement;
            final String exitStatusFromJSL = failElement.getExitStatus();
            retVal = new ExecutionStatus(ExtendedBatchStatus.JSL_FAIL);
            if (exitStatusFromJSL != null) {
                jobContext.setExitStatus(exitStatusFromJSL);
                retVal.setExitStatus(exitStatusFromJSL);
            }
        } else {
            throw new IllegalStateException("Not sure how we'd get here...aborting.");
        }
        return retVal;
    }

    public Controller getCurrentStoppableElementController() {
        return currentStoppableElementController;
    }

    public List<Long> getStepExecIds() {
        return stepExecIds;
    }
}