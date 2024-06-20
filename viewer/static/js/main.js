$(document).ready(function() {
    $('form').submit(function(event) {
        event.preventDefault();  // Prevent the default form submission
        
        // Show the loading sign with animation
        $('#loading').show();
        
        $.post('/search', $('form').serialize(), function(data) {
            // Update the results placeholder with the received HTML table
            updateResults(data.html_table);
            updateSummary(data.summary);
            updateResultsSummaryInfo(data.results_summary_info);
            $('#searchResults').show();
            // Hide the loading sign after results are loaded
            $('#loading').hide();
            
        });
    });

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
    


    $(function() {
        createYearSlider();
    
        $('button[type="reset"]').click(function() {
            // Remove the old sliders
            $('#yearSlider').slider('destroy');
    
            // Create new sliders
            createYearSlider();
        });
    });
});

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
        values: [2000, 2024],
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
        $('#searchResultsSummary').toggle();
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
        paging: false,
        searching: false,
        order: [[0, 'desc']],
    });
}

function updateResultsSummaryInfo(summary) {
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