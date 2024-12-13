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
    clearLoadingInfo();
    $('#loading').show();
    startLoadingTimer();
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

let last_found_status = null
let loadingStartTime = null;
let loadingTimerInterval = null;

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
            clearLoadingInfo()
        } else if (data.status === 'failed') {
            console.log("  error: " + data.result)
            $('#searchErrorMessage').text("Error trying to conduct the search: " + data.result).show();
            clearLoadingInfo()
        } else if (data.status === 'in progress') {
            $('#loadingDesc').text(data.status_desc)
            setTimeout(function () {
                checkSearchStatus(taskId);
            }, 500);
            last_found_status = Date.now()
        } else if (data.status === 'not found') {
            if (last_found_status) {
                if (Date.now() - last_found_status > 120000) {
                    $('#searchErrorMessage').text("Search timed out please try again").show();
                    clearLoadingInfo()
                } else {
                    setTimeout(function () {
                        checkSearchStatus(taskId);
                    }, 500);
                }
            }
        }
    });
}
function startLoadingTimer() {
    loadingStartTime = Date.now(); // Record the start time
    loadingTimerInterval = setInterval(function () {
        const elapsedTime = Math.floor((Date.now() - loadingStartTime) / 1000); // Elapsed time in seconds
        $('#loadingDuration').text('Search duration: ' + elapsedTime + ' seconds');
    }, 1000); // Update every second
}

function clearLoadingInfo() {
    clearInterval(loadingTimerInterval); // Stop the timer
    $('#loadingDuration').text('');
    $('#loading').hide(); // Hide the loading indicator
    $('#loadingDesc').text("");
    last_found_status = null;
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

    $('#quickSearchSummary').text(summary.quick_search_summary)

    $('#resultsSummaryText').replaceWith("<p>Search took " + summary.duration + "</p>");

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