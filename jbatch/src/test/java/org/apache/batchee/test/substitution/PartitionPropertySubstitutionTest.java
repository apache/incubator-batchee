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
package org.apache.batchee.test.substitution;

import org.apache.batchee.container.impl.JobInstanceImpl;
import org.apache.batchee.container.services.persistence.jpa.domain.PropertyHelper;
import org.apache.batchee.util.Batches;
import org.testng.annotations.Test;

import javax.batch.api.AbstractBatchlet;
import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.AbstractItemReader;
import javax.batch.api.chunk.AbstractItemWriter;
import javax.batch.api.partition.AbstractPartitionAnalyzer;
import javax.batch.api.partition.PartitionCollector;
import javax.batch.api.partition.PartitionMapper;
import javax.batch.api.partition.PartitionPlan;
import javax.batch.api.partition.PartitionPlanImpl;
import javax.batch.operations.JobOperator;
import javax.batch.runtime.BatchRuntime;
import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.Metric;
import javax.batch.runtime.StepExecution;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class PartitionPropertySubstitutionTest {

    public final static String STEP_PROP = "stepProp";
    public final static String STEP_PROP_VAL = "stepPropVal";
    public final static String STEP_CONTEXT_PROPERTY = "STEP_CONTEXT_PROPERTY";
    public final static String SUBSTITUTION_PROPERTY = "SUBSTITUTION_PROPERTY";
    
	@Test
	public void testPartitionPropertyResolverForCollector() throws Exception {
        final JobOperator op = BatchRuntime.getJobOperator();
        Properties jobParams = new Properties();
        jobParams.setProperty(STEP_PROP, STEP_PROP_VAL);
		final long id = op.start("partition-propertyResolver", jobParams);
        Batches.waitForEnd(op, id);

        StepExecution stepExecution = op.getStepExecutions(id).iterator().next();

		assertEquals(op.getJobExecution(id).getBatchStatus(), BatchStatus.COMPLETED);
        assertNotNull(op.getJobExecution(id).getJobParameters());
        assertEquals(jobParams, op.getJobExecution(id).getJobParameters());
        for (final JobExecution exec: op.getJobExecutions(new JobInstanceImpl(id))) {
            assertNotNull(exec.getJobParameters());
            assertEquals(jobParams, exec.getJobParameters());
        }

		ArrayList<String> data = (ArrayList<String>)stepExecution.getPersistentUserData();

		for (String nextStringifiedProps : data) {
			Properties props = PropertyHelper.stringToProperties(nextStringifiedProps);
			String valFromStepProp = props.getProperty(STEP_CONTEXT_PROPERTY);
			String valFromSubstitution = props.getProperty(SUBSTITUTION_PROPERTY);
			assertEquals(valFromStepProp, STEP_PROP_VAL, "Compare values from step-level property with param used in substitution");
			assertEquals(valFromSubstitution, STEP_PROP_VAL, "Compare values from step-level property with a collector-property using this step-level property via a 'jobProperties' substitution.");
		}
	}
	
	@Test
	public void testPartitionPropertyResolverForMapper() throws Exception {
        final JobOperator op = BatchRuntime.getJobOperator();
        Properties jobParams = new Properties();
        jobParams.setProperty(STEP_PROP, STEP_PROP_VAL);
		final long id = op.start("partition-propertyResolver", jobParams);
        Batches.waitForEnd(op, id);

		assertEquals(op.getJobExecution(id).getBatchStatus(), BatchStatus.COMPLETED);

		String exitStatus = op.getJobExecution(id).getExitStatus();

		Properties props = PropertyHelper.stringToProperties(exitStatus);

		String valFromStepProp = props.getProperty(STEP_CONTEXT_PROPERTY);
		String valFromSubstitution = props.getProperty(SUBSTITUTION_PROPERTY);
		assertEquals(valFromStepProp, STEP_PROP_VAL, "Compare values from step-level property with param used in substitution");
		assertEquals(valFromSubstitution, STEP_PROP_VAL, "Compare values from step-level property with a collector-property using this step-level property via a 'jobProperties' substitution.");
	}
	
	/*
	@Test
	public void testMapperPropertyResolver() throws Exception {
		long execId = jobOp.start("partitionPropertyResolverMapperTest", null);
		Thread.sleep(sleepTime);
		JobExecution je = jobOp.getJobExecution(execId);
		
		assertEquals("batch status", BatchStatus.COMPLETED, je.getBatchStatus());
		
		List<StepExecution> stepExec = jobOp.getStepExecutions(execId);
		String stepProp2Data = stepExec.get(0).getExitStatus();
		String partitionAndStepPropData = String.valueOf(stepExec.get(0).getPersistentUserData());
		
		String[] prop2Tokens = stepProp2Data.split(":");
		String stepProp2Value = prop2Tokens[1];
		int partitionsTotal = Integer.parseInt(prop2Tokens[2]);
		
		String[] tokens = partitionAndStepPropData.split("#");
		String[] partitionStepPropValues = new String[tokens.length - 1];
		
		for(int i=1; i < tokens.length; i++) {
			partitionStepPropValues[i - 1] = tokens[i];
		}
		
		int count = 0;
		for(String s : partitionStepPropValues) {
			String stepPropsValue = s.substring(s.indexOf("?") + 1);
			assertEquals("StepProp2's Value ", stepProp2Value, stepPropsValue);

			String partitionStringsValue = s.substring(0, s.indexOf("?"));
			assertEquals("PartitionString's Value ", partitionStringsValue, stepPropsValue);
			count++;
		}

		assertEquals("Paritions seen ", count, partitionsTotal);
	}
	*/
	
    public static class Analyzer extends AbstractPartitionAnalyzer {

    	@Inject
    	StepContext stepCtx;
    	
    	@Override
    	public void analyzeCollectorData(Serializable data) throws Exception {
    		ArrayList<String> currentData = (ArrayList<String>)stepCtx.getPersistentUserData();
    		if (currentData == null) {
    			currentData = new ArrayList<String>();
    		}
    		currentData.add((String)data);

    		// Set in persistent user data for later test validation
    		stepCtx.setPersistentUserData(currentData);
    	}
    }

    public static class Batchlet extends AbstractBatchlet{
    	@Override
    	public String process() throws Exception {	
    		return "GOOD";
    	}
    }
    
    public static class Collector implements PartitionCollector {

    	@Inject
    	StepContext stepCtx;
    	
    	@Inject @BatchProperty
    	String substitutedWithStepPropValue;
    	
    	@Override
    	public String collectPartitionData() throws Exception {
    		Properties props = new Properties();
    		String stepCtxPropVal = stepCtx.getProperties().getProperty(STEP_PROP);
    		props.setProperty(STEP_CONTEXT_PROPERTY, stepCtxPropVal);
    		props.setProperty(SUBSTITUTION_PROPERTY, substitutedWithStepPropValue);
    		return PropertyHelper.propertiesToString(props);
    	}
    }
    
    public static class Mapper implements PartitionMapper{
    	
    	@Inject
    	JobContext jobCtx;
    	
    	@Inject
    	StepContext stepCtx;
    	
    	@Inject @BatchProperty
    	String substitutedWithStepPropValue;

    	@Override
    	public PartitionPlan mapPartitions() throws Exception {

    		PartitionPlanImpl pp = new PartitionPlanImpl();
    		pp.setPartitions(3);
    		Properties p0 = new Properties();
    		Properties p1 = new Properties();
    		Properties p2 = new Properties();
    		Properties[] partitionProps = new Properties[3];
    		partitionProps[0] = p0;
    		partitionProps[1] = p1;
    		partitionProps[2] = p2;
    		pp.setPartitionProperties(partitionProps);
    		
    		Properties props = new Properties();
    		String stepCtxPropVal = stepCtx.getProperties().getProperty(STEP_PROP);
    		System.out.println("SKSK: in mapper, stepCtxPropVal = " + stepCtxPropVal);
    		System.out.println("SKSK: in mapper, substitutedWithStepPropValue = " + substitutedWithStepPropValue);
    		props.setProperty(STEP_CONTEXT_PROPERTY, stepCtxPropVal);
    		props.setProperty(SUBSTITUTION_PROPERTY, substitutedWithStepPropValue);
    		
    		// Set in exit status for later test validation
    		jobCtx.setExitStatus(PropertyHelper.propertiesToString(props));

    		return pp;
    	}

    }
}

