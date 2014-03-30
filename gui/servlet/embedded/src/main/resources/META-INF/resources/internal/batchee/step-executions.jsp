<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<%@ page session="false" %>
<%@ page import="org.apache.batchee.servlet.MetricsHelper" %>
<%@ page import="javax.batch.runtime.StepExecution" %>
<%@ page import="java.util.List" %>
<%@ page import="org.apache.batchee.servlet.StatusHelper" %>

<% final String name = (String) request.getAttribute("name"); %>
<h4>Step executions for execution #<%= request.getAttribute("executionId") %> of <%= name %></h4>

<table class="table tabl-hover">
    <thead>
    <tr>
        <th>#</th>
        <th>Step name</th>
        <th>Batch status</th>
        <th>Exit status</th>
        <th>Start time</th>
        <th>End time</th>
        <th>Metrics</th>
    </tr>
    </thead>
    <tbody>
<% for ( final StepExecution step : (List<StepExecution>) request.getAttribute("steps") ) { %>
    <tr class="<%= StatusHelper.statusClass(step.getBatchStatus()) %>">
        <td><%= step.getStepExecutionId() %></td>
        <td><%= step.getStepName() %></td>
        <td><%= step.getBatchStatus().name() %></td>
        <td><%= step.getExitStatus() %></td>
        <td><%= step.getStartTime() %></td>
        <td><%= step.getEndTime() %></td>
        <td><%= MetricsHelper.toString(step.getMetrics()) %></td>
    </tr>
<% } %>
    </tbody>
</table>
