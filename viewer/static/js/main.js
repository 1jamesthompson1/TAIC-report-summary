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
        $('#advancedSearch').toggle();
        
    });
        
});

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
        var searchQuery = $('#searchQuery').val();

        $.get('/get_report_text', {report_id: reportId, search_query: searchQuery}, function(data) {
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