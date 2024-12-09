/* --------------------------------------------------------------------------
//  To maintain readability of the JS all of the code that is relate to the
//  search is contained in this file.
// -------------------------------------------------------------------------- */
export function formSubmission(event) {
    event.preventDefault(); 
    if (!checkBoxesAreTicked()) {
        setSearchErrorMessage('Please select at least one checkbox');
        return false;
    } else {
        setSearchErrorMessage('');
    }
    
    // Show the loading sign with animation
    $('#loading').show();
    console.log("Conducting search")
    
    
    // Clear previous results
    $('#searchResults').hide();
    
    $.post('/search', $('form').serialize(), function (data) {
    
        // Get the task ID from the response
        const taskId = data.task_id;
        // Poll for task status
        checkSearchStatus(taskId);
    });
}

function checkSearchStatus(taskId) {
    console.log("Checking status: " + taskId)
    $.get('/task-status/' + taskId, function (data) {
        console.log("  status: " + data.status)
        if (data.status === 'completed') {
            console.log("  Search completed")
            updateResults(data.result.html_table);
            updateSummary(data.result.summary);
            updateResultsSummaryInfo(data.result.results_summary_info);
            $('#searchResults').show();
            $('#loading').hide();
        } else if (data.status === 'failed') {
            console.log("  error: " + data.result)
            $('#searchErrorMessage').text("Error trying to conduct the search: " + data.result).show();
            $('#loading').hide();
        } else {
            setTimeout(function () {
                checkSearchStatus(taskId);
            }, 2000);
        }
    });
}
function setSearchErrorMessage(message) {
    if (message == '') {
        $('#searchErrorMessage').text('').hide();
    } else {
        $('#searchErrorMessage').text(message).show();
        $('#searchErrorMessage').css('color', 'red');
    }
}
function atLeastOneChecked($group) {
    return $group.find('input[type="checkbox"]:checked').length > 0;
}
function checkBoxesAreTicked() {
    /** Check each checkbox group and make sure that atleast one is ticked */
    let checkBoxesPass = true
    $('.checkbox-group.required').each(function () {
        if (!atLeastOneChecked($(this))) {
            checkBoxesPass = false
        }
    });
    return checkBoxesPass;
}


function updateSummary(summary) {
    if (!summary) {
        $('#searchResultsSummary').hide();
        return;
    }
    $('#searchResultsSummary').show();
    $('#searchResultsSummaryText').html(marked.parse(summary));
}

function updateResults(htmlTable) {
    $('#searchResultsTableWrapper').html(htmlTable);

    $('#dataTable').DataTable({
        autoWidth: false,
        fixedColumns: true,
        fixedHeader: true,
        paging: true,
        searching: true,
        order: [[0, 'desc']],
    });
}

function updateResultsSummaryInfo(summary) {

    $('#resultsSummaryText').replaceWith("<p>Found " + summary.num_results + " relevant documents in " + summary.duration + " minutes</p>");

    var most_common_document_types = JSON.parse(summary.document_type_pie_chart);
    Plotly.newPlot('MostCommmonDocumentTypes', most_common_document_types);
    var most_common_event_types = JSON.parse(summary.most_common_event_types);

    Plotly.newPlot('MostCommonEventTypes', most_common_event_types);

    var mode_pie_chart = JSON.parse(summary.mode_pie_chart);

    Plotly.newPlot('ModePieChart', mode_pie_chart);

    var year_histogram = JSON.parse(summary.year_histogram);

    Plotly.newPlot('YearHistogram', year_histogram);

    var agency_pie_chart = JSON.parse(summary.agency_pie_chart);

    Plotly.newPlot('AgencyPieChart', agency_pie_chart);
}