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
<%@ page import="java.net.URLEncoder" %>
<%@ page import="org.apache.batchee.servlet.JBatchController" %>
<%@ page session="false" %>

<%
    request.setAttribute("pageJs", "start.js");

    final String name = (String) request.getAttribute("name");
    final boolean newJob = name == null || name.isEmpty();
    if (newJob) {
%>
<div>
    Add needed job parameters and name for the new job then click on submit:
</div>
<% } else { %>
<div>
    Add needed job parameters for job <b><%= name %></b> then click on submit:
</div>
<% } %>

<% if (newJob) { %>
<div class="control-group">
    <div class="controls" id="c_' + key + '">
        <input type="text" id="job-name-input" placeholder="type job name...">
        <button id="set-job-name" class="btn btn-small" type="button">Set Job Name</button>
    </div>
</div>
<% } %>

<div class="control-group">
  <div class="controls">
      <input type="text" id="key" placeholder="type a key...">
      <input type="text" id="value" placeholder="type a value...">
      <button id="add-param" class="btn btn-small" type="button">Add</button>
  </div>
</div>

<form action="<%= request.getAttribute("mapping") %>/doStart/<%= URLEncoder.encode(name, "UTF-8") %>" method="POST"
      class="form-horizontal">
    <div id="values"></div>
    <input id="job-name" type="hidden" name="<%= JBatchController.FORM_JOB_NAME %>" value="<%= name %>">
    <button id="start-job" type="submit" class="btn">Submit</button>
</form>
