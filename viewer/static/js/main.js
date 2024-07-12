$(document).ready(function() {
    $('form').submit(function(event) {
        event.preventDefault();  // Prevent the default form submission

        if (!checkBoxesAreTicked()) {
            setSearchErrorMessage('Please select at least one checkbox');
            return false;
        } else {
            setSearchErrorMessage('');
        }
        
        // Show the loading sign with animation
        $('#loading').show();
        
        $.post('/search', $('form').serialize(), function(data) {
            // Get the task ID from the response
            const taskId = data.task_id;
            // Poll for task status
            checkStatus(taskId);
        });
    });

    function checkStatus(taskId) {
        $.get('/task-status/' + taskId, function(data) {
            if (data.status === 'completed') {
                // Update the results placeholder with the received HTML table
                updateResults(data.result.html_table);
                updateSummary(data.result.summary);
                updateResultsSummaryInfo(data.result.results_summary_info);
                $('#searchResults').show();
                // Hide the loading sign after results are loaded
                $('#loading').hide();
            } else if (data.status === 'failed') {
                // Show an error message
                $('#searchErrorMessage').text("Error trying to conduct the search: " + data.result).show();
                // Hide the loading sign
                $('#loading').hide();
            } else {
                setTimeout(function() {
                    checkStatus(taskId);
                }, 2000); 
            }
        });
    }

    $('[id^="downloadCSVBtn"]').click(function() {
        var actionUrl = $(this).attr('id').replace('downloadCSVBtn', '/get') + '_as_csv';
        downloadCSV(actionUrl);
    });
    
    // Add logic for toggle button to show/hide summary results info
    $('#toggleResultsSummaryInfo').click(function() {
        $('#searchResultsSummaryInfo').toggle();
    });

    $('#advancedSearchBtn').click(function() {
        $('#resetBtn').toggle();
        $('#advancedSearch').toggleClass('expanded')
        if ($('#advancedSearch').css('display') == 'flex') {
            $('#advancedSearch').css('display', 'none')
        } else {
            $('#advancedSearch').css('display', 'flex')
        }
    });
    
    $('#searchQuery').on('input', function() {
        const textLength = $(this).val().length;
        const minWidth = 30;
        const maxWidth = 200;
        const requiredWidth = textLength * 1.2; // Adjust the multiplier as needed

        const newWidth = Math.min(Math.max(minWidth, requiredWidth), maxWidth);
        $(this).css('width', newWidth + 'ch');
    });

    $('#relevanceCutoffSlider').on('input', function() {
        const value = $(this).val();
        $('#relevanceCutoffSliderLabel').text(value);

    });


    $(function() {
        createYearSlider();
    
        $('button[type="reset"]').click(function() {
            // Remove the old sliders
            $('#yearSlider').slider('destroy');
            // Remove hidden inputs within the sliders
            $('#yearSlider input[type="hidden"]').remove();
            // Create new sliders
            createYearSlider();

            // Reset the form
            $('form').trigger('reset');

            $('#relevanceCutoffSliderLabel').text($('#relevanceCutoffSlider').val());

        });
    });
});
function setSearchErrorMessage(message) {
    if (message == '') {
        $('#searchErrorMessage').hide();
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
    checkBoxesPass = true
    $('.checkbox-group.required').each(function() {
        if (!atLeastOneChecked($(this))) {
            checkBoxesPass = false
        }
    });
    return checkBoxesPass;
}

function downloadCSV(actionUrl) {
    var formData = $('form').serialize();
    var form = $('<form>', {
        action: actionUrl,
        method: 'post'
    });
    $.each(formData.split('&'), function(i, v) {
        var parts = v.split('=');
        form.append($('<input>', {
            type: 'hidden',
            name: decodeURIComponent(parts[0]),
            value: decodeURIComponent(parts[1])
        }));
    });
    form.appendTo('body').submit().remove();
}


function createYearSlider() {
    const $minInput = $('<input>').attr({ type: 'hidden', name: `yearSlider-min` });
    const $maxInput = $('<input>').attr({ type: 'hidden', name: `yearSlider-max` });
    $('#yearSlider').append($minInput, $maxInput);
    $("#yearSlider").slider({
        range: true,
        min: 2000,
        max: 2024,
        values: [2007, 2024],
        create: function() {
            // Add divs to the handles
            $(this).children('.ui-slider-handle').each(function(i) {
                $(this).append($('<div>').addClass('handle-value').text($("#yearSlider").slider('values', i)));
            });
           // Update the hidden inputs with the slider values
            $minInput.val($("#yearSlider").slider('values', 0));
            $maxInput.val($("#yearSlider").slider('values', 1));
        },
        slide: function(_, ui) {
            // Update the text of the handle divs
            $(this).children('.ui-slider-handle').each(function(i) {
                $(this).children('.handle-value').text(ui.values[i]);
            });

            // Update the hidden inputs with the slider values
            $minInput.val(ui.values[0]);
            $maxInput.val(ui.values[1]);
            }
    });
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
        searching: false,
        order: [[0, 'desc']],
    });
    }

    function updateResultsSummaryInfo(summary) {
    var most_common_document_types = JSON.parse(summary.document_type_pie_chart);
    Plotly.newPlot('MostCommmonDocumentTypes', most_common_document_types);
    var most_common_event_types = JSON.parse(summary.most_common_event_types);

    Plotly.newPlot('MostCommonEventTypes', most_common_event_types);
    
    var mode_pie_chart = JSON.parse(summary.mode_pie_chart);

    Plotly.newPlot('ModePieChart', mode_pie_chart);

    var year_histogram = JSON.parse(summary.year_histogram);
    
    Plotly.newPlot('YearHistogram', year_histogram);
    }

    // -----   Popups on results table ----- //
    

    function openReportPopup(data) {
        // Set the content of the modal
        $('#modalText').html(data.main);
        
    $('#modalTitle').html(data.title);

    // Display the modal
    $('#modalContainer').css('display', 'block');
}

function closeModal() {
// Hide the modal
    $('#modalContainer').css('display', 'none');
}

$(document).ready(function() {     
    $(document).on('click', '.safety-issue-recommendations-link', function() {
        var safety_issue_id = $(this).data('safety-issue-id');
    
        $.get('/get_safety_issue_recommendations', {safety_issue_id: safety_issue_id}, function(data) {
            openReportPopup(data);
        })

        return false
    
    });
    
    
    $(document).on('click', '.links-visual-link', function() {
        var reportId = $(this).data('report-id');
        
        $.get('/get_links_visual', {report_id: reportId}, function(data) {
            openReportPopup(data);
        })
        return false
    });

    $('#closeModal').click(function() {
        closeModal();
    });

    $(document).on('click', function(event) {
        if (event.target === $('#myModal')[0]) {
        closeModal();
        }
    });

    $(document).on('keydown', function(event) {
        if (event.key === 'Escape') {
        closeModal();
        }
    });

    $('#closeModal').click(function() {
        closeModal();
    });
        
});