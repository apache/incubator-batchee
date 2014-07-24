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
$(function () {
    $('#add-param').click(function () {
        var keyEntry = $('#key');
        var valueEntry = $('#value');

        var key = keyEntry.val();
        var value = valueEntry.val();

        $('#values').append('' +
            '<div id="cg_' + key + '" class="control-group">' +
            '  <div class="controls" id="c_' + key + '">' +
            '    <input type="text" name="k_' + key + '" value="' + key + '">' +
            '    <input type="text" name="v_' + key + '" value="' + value + '">' +
            '    <button id="cg_' + key + 'Remove" class="btn btn-small" type="button">Remove</button>' +
            '  </div>' +
            '</div>');

        keyEntry.val('');
        valueEntry.val('');

        var blockId = '#cg_' + key;
        $(blockId + 'Remove').on('click', function(event) {
            event.preventDefault();
            $(blockId).remove();
        });
    });

    $('#set-job-name').on('click', function(event) {
        event.preventDefault();
        $('#job-name').val($('#job-name-input').val());
    });

    $('#start-job').submit(function (event) {
        var jobNameInput = $('#job-name-input');
        if (jobNameInput.length) {
            $('#job-name').val(jobNameInput.val());
        }
    });
});
