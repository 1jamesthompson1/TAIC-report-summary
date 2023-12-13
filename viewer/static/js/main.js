$(document).ready(function() {
    $('form').submit(function(event) {
        event.preventDefault();  // Prevent the default form submission
        
        // Show the loading sign with animation
        $('#loading').show();
        
        $.post('/search', $('form').serialize(), function(data) {
            // Update the results placeholder with the received HTML table
            updateResults(data.html_table);
            // Hide the loading sign after results are loaded
            $('#loading').hide();
            
        });
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
    
    $('#expandThemeSlidersBtn').click(function() {
        $('#themeSliders').toggleClass('expanded');
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
    $.get('/get_theme_titles', (data) => {
        const themeTitles = data.theme_titles;
        const $themeSliders = $('#themeSliders');
    
        // Create a range slider for each theme title
        themeTitles.forEach((title) => {
            const $label = $('<label>').text(title).addClass('slider-label');

            const $div = $('<div>').attr({ id: `theme-${title}` }).addClass('slider');
            const $sliderWrapper = $('<div>').addClass('slider-wrapper');

            const $minInput = $('<input>').attr({ type: 'hidden', name: `theme-${title}-min` });
            const $maxInput = $('<input>').attr({ type: 'hidden', name: `theme-${title}-max` });
    
            $sliderWrapper.append($label, $div, $minInput, $maxInput);
            $themeSliders.append($sliderWrapper);
    
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
    });
}

function updateResults(htmlTable) {
    document.getElementById('searchResults').innerHTML = htmlTable;
    $('#dataTable').DataTable( {
        autoWidth: false,
        fixedColumns: true,
        fixedHeader: true,
        paging: false,
        searching: false,
        "order": [ 1, 'dsc' ],
        "columnDefs": [ 
            { "targets": 2, "orderable": false},
        ]
    } );
}

// -----   Popups on results table ----- //


function openReportPopup(data) {
    // Set the content of the modal
    $('#modalContent').html(data.main);

    $('#modalTitle').html(data.title);

    // Display the modal
    $('#myModal').css('display', 'block');
}

function closeModal() {
// Hide the modal
    $('#myModal').css('display', 'none');
}

$(document).ready(function() {     
    $(document).on('click', '.no-matches-link', function() {
        var reportId = $(this).data('report-id');
        var formData = $('#searchForm').serialize();
        
        $.get('/get_report_text', {report_id: reportId, form: formData}, function(data) {
            openReportPopup(data);
        })
    });

    $(document).on('click', '.weighting-link', function() {
        var reportId = $(this).data('report-id');
        var theme = $(this).data('theme');

        $.get('/get_weighting_explanation', {report_id: reportId, theme: theme}, function(data) {
            openReportPopup(data);
        })
    });

    $(document).on('click', '.theme-summary-link', function() {
        var reportId = $(this).data('report-id');

        $.get('/get_theme_text', {report_id: reportId}, function(data) {
            openReportPopup(data);
        })
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