$(document).ready(function() {
    $('form').submit(function(event) {
        event.preventDefault();  // Prevent the default form submission
        
        // Show the loading sign with animation
        $('#loading').show();
        
        $.post('/search', $('form').serialize(), function(data) {
            // Update the results placeholder with the received HTML table
            updateResults(data.html_table);
            $('#searchResultsHeader').show();
            // Hide the loading sign after results are loaded
            $('#loading').hide();
            
        });
    });

    $('#downloadResultsSummary').click(function() {
        $('#loading').show();   

        var formData = $('form').serialize();
        $.ajax({
            type: 'POST',
            url: '/get_results_summary_report',
            data: formData,
            success: function(data) {
                checkResult();
            }
        });
    });
    
    function checkResult() {
        $.ajax({
            type: 'GET',
            url: '/get_result',
            success: function(data, status, xhr) {
                if (xhr.status === 202) {
                    setTimeout(checkResult, 1000);
                } else {
                    $('#loading').hide();
                    window.location.href = '/get_result';
                }
            }
        });
    }

    $('#downloadResultsCSV').click(function() {
        var formData = $('form').serialize();
        var form = $('<form>', {
            action: '/get_results_as_csv',
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
        createThemeSliders();
    
        $('button[type="reset"]').click(function() {
            // Remove the old sliders
            $('#yearSlider').slider('destroy');
            $('#themeSliders').empty();
    
            // Create new sliders
            createYearSlider();
            createThemeSliders();
        });
    });
});

$(document).on('click', '.showIndividualThemeSlidersBtn', function() {
    $(this).siblings('.indivudalThemeSliders').toggle();

    if ($(this).text() == 'Expand to individual themes') {
        $(this).text('Collapse to theme group');
    }
    else {
        $(this).text('Expand to individual themes');
    }
});

function createYearSlider() {
    const $minInput = $('<input>').attr({ type: 'hidden', name: `yearSlider-min` });
    const $maxInput = $('<input>').attr({ type: 'hidden', name: `yearSlider-max` });
    $('#yearSlider').append($minInput, $maxInput);
    $("#yearSlider").slider({
        range: true,
        min: 2010,
        max: 2022,
        values: [2010, 2022],
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

function createThemeSliders() {
    $.get('/get_theme_groups', (data) => {
        const themeGroups = data.themeGroups;
        const $themeSliders = $('#themeSliders');
    
        // Create a range slider for each theme title
        themeGroups.forEach((group) => {
            const $groupDiv = $('<div>').addClass('theme-group');
            const $groupLabel = $('<label>').text(group.title).addClass('theme-group-label');

            // Add overall group slider
            const $div = $('<div>').attr({ id: `theme-group-slider-${group.title}` }).addClass('slider');
            const $sliderWrapper = $('<div>').addClass('slider-wrapper theme-group-slider-wrapper');

            $div.slider({
                range: true,
                min: 0,
                max: 100,
                values: [0, 100],
                create: function() {
                    // Add divs to the handles
                    $(this).children('.ui-slider-handle').each(function(i) {
                        $(this).append($('<div>').addClass('handle-value').text($div.slider('values', i)));
                    });
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

            const $minInput = $('<input>').attr({ type: 'hidden', name: `theme-group-${group.title}-min` });
            const $maxInput = $('<input>').attr({ type: 'hidden', name: `theme-group-${group.title}-max` });

            $minInput.val($div.slider('values', 0));
            $maxInput.val($div.slider('values', 1));
            $sliderWrapper.append($div, $minInput, $maxInput);
            $groupDiv.append($groupLabel, $sliderWrapper);

            const $themeSlidersDiv = $('<div>').addClass('indivudalThemeSliders')

            const $expandButton = $('<button>').addClass('showIndividualThemeSlidersBtn').text('Expand to individual themes').attr({ type: 'button' });

            group.themes.forEach((title) => {
                const $label = $('<label>').text(title).addClass('slider-label');


                const $div = $('<div>').attr({ id: `theme-${title}` }).addClass('slider');
                const $sliderWrapper = $('<div>').addClass('slider-wrapper');

                const $minInput = $('<input>').attr({ type: 'hidden', name: `theme-${title}-min` });
                const $maxInput = $('<input>').attr({ type: 'hidden', name: `theme-${title}-max` });
        
                $sliderWrapper.append($label, $div, $minInput, $maxInput);
                $themeSlidersDiv.append($sliderWrapper);

        
                // Initialize the range slider
                $div.slider({
                    range: true,
                    min: 0,
                    max: 100,
                    values: [0, 100],
                    create: function() {
                        // Add divs to the handles
                        $(this).children('.ui-slider-handle').each(function(i) {
                            $(this).append($('<div>').addClass('handle-value').text($div.slider('values', i)));
                        });
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
                
                // Set text and hidden input to default value
                $minInput.val($div.slider('values', 0));
                $maxInput.val($div.slider('values', 1));

            });

            $groupDiv.append($expandButton,$themeSlidersDiv);
            $themeSliders.append($groupDiv);
        });
    });
}

function updateResults(htmlTable) {
    $('#searchResultsTableWrapper').html(htmlTable);
    $('#dataTable').DataTable({
        autoWidth: false,
        fixedColumns: true,
        fixedHeader: true,
        paging: false,
        searching: false,
        order: [1, 'desc'],
        columnDefs: [
            { targets: 2, orderable: false }
        ]
    });
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
    $(document).on('click', '.no-matches-link', function() {    
        var reportId = $(this).data('report-id');
        var formData = $('#searchForm').serialize();
         
        $.get('/get_report_text', {report_id: reportId, form: formData}, function(data) {

            openReportPopup(data);
        })
        return false;
    });

    $(document).on('click', '.weighting-link', function() {
        var reportId = $(this).data('report-id');
        var theme = $(this).data('theme');

        $.get('/get_weighting_explanation', {report_id: reportId, theme: theme}, function(data) {
            openReportPopup(data);
        })
        return false
    });

    $(document).on('click', '.theme-summary-link', function() {
        var reportId = $(this).data('report-id');

        $.get('/get_theme_text', {report_id: reportId}, function(data) {
            openReportPopup(data);
        })
        return false
    });

    $(document).on('click', '.safety-issues-link', function() {
        var reportId = $(this).data('report-id');

        $.get('/get_safety_issues', {report_id: reportId}, function(data) {
            openReportPopup(data);
        })
        return false
        
    });

    $(document).on('click', '.recommendations-link', function() {
        var reportId = $(this).data('report-id');
        
        $.get('/get_recommendations', {report_id: reportId}, function(data) {
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