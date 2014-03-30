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
<% final String mapping = (String) request.getAttribute("mapping"); %>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="Apache JBatch GUI">

    <link href="<%= request.getAttribute("context") %>/internal/batchee/css/bootstrap.min.3.0.0.css" rel="stylesheet" media="screen">

    <title>Apache JBatch GUI</title>
</head>

<body>

<div class="container">
    <div class="header">
        <ul class="nav nav-pills pull-right">
            <li><a href="<%= mapping %>/start/">New Batch</a></li>
            <li><a href="<%= mapping %>/">Home</a></li>
        </ul>
        <h3 class="text-muted">Apache JBatch GUI</h3>
    </div>

    <div id="content" class="row">
        <jsp:include page="/internal/batchee/${requestScope.view}.jsp" />
    </div>

    <div class="footer">
        <p>Apache &copy; Company 2013</p>
    </div>
</div>

<script src="<%= request.getAttribute("context") %>/internal/batchee/js/jquery-2.0.3.min.js"></script>
<script src="<%= request.getAttribute("context") %>/internal/batchee/js/bootstrap.min.3.0.0.js"></script>
<%
    final String pageJs = (String) request.getAttribute("pageJs");
    if (pageJs != null) {
        for (final String js : pageJs.split(",")) {
%>
<script src="<%= request.getAttribute("context") %>/internal/batchee/js/<%= js %>"></script>
<%
        }
    }
%>
</body>
</html>
