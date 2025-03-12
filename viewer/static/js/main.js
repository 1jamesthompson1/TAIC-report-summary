import { formSubmission } from "./search.js";


$(document).ready(function () {
    if (hasSearchParams()) {
        const params = getUrlParams();
        initializeFormWithParams(params);
    }

    setupEventHandlers();
    createYearSlider();
});


function setupEventHandlers() {
    $('form').submit(formSubmission);

    $('[id^="downloadCSVBtn"]').click(function () {
        const actionUrl = $(this).attr('id').replace('downloadCSVBtn', '/get') + '_as_csv';
        downloadCSV(actionUrl);
    });

    $('#toggleResultsSummaryInfo').click(function () {
        $('#searchResultsSummaryInfo').toggle();
    });

    $('#advancedSearchBtn').click(function () {
        $('#resetBtn').toggle();
        $('#advancedSearch').toggleClass('expanded')
            .css('display', $('#advancedSearch').css('display') === 'flex' ? 'none' : 'flex');
    });

    $('#searchQuery').on('input', function () {
        const textLength = $(this).val().length;
        const minWidth = 30;
        const maxWidth = 200;
        const requiredWidth = textLength * 1.2;
        const newWidth = Math.min(Math.max(minWidth, requiredWidth), maxWidth);
        $(this).css('width', newWidth + 'ch');
    });

    $('#relevanceCutoffSlider').on('input', function () {
        $('#relevanceCutoffSliderLabel').text($(this).val());
    });

    $('button[type="reset"]').click(function () {
        $('#yearSlider').slider('destroy');
        $('#yearSlider input[type="hidden"]').remove();
        createYearSlider();
        $('form').trigger('reset');
        $('#relevanceCutoffSliderLabel').text($('#relevanceCutoffSlider').val());
    });
}

function createYearSlider() {
    const $minInput = $('<input>').attr({ type: 'hidden', name: `yearSlider-min` });
    const $maxInput = $('<input>').attr({ type: 'hidden', name: `yearSlider-max` });
    const endyear = new Date().getFullYear()
    $('#yearSlider').append($minInput, $maxInput);
    $("#yearSlider").slider({
        range: true,
        min: 2000,
        max: endyear,
        values: [2007, endyear],
        create: function () {
            // Add divs to the handles
            $(this).children('.ui-slider-handle').each(function (i) {
                $(this).append($('<div>').addClass('handle-value').text($("#yearSlider").slider('values', i)));
            });
            // Update the hidden inputs with the slider values
            $minInput.val($("#yearSlider").slider('values', 0));
            $maxInput.val($("#yearSlider").slider('values', 1));
        },
        slide: function (_, ui) {
            // Update the text of the handle divs
            $(this).children('.ui-slider-handle').each(function (i) {
                $(this).children('.handle-value').text(ui.values[i]);
            });

            // Update the hidden inputs with the slider values
            $minInput.val(ui.values[0]);
            $maxInput.val(ui.values[1]);
        }
    });
}

/* ---------------------------------------------------
         Handling search params in the URL
--------------------------------------------------- */

function hasSearchParams() {
    return window.location.search.length > 0;
}

function getUrlParams() {
    const params = new URLSearchParams(window.location.search);
    let formData = {};
    for (const [key, value] of params.entries()) {
        formData[key] = value;
    }
    return formData;
}

function initializeFormWithParams(params) {
    Object.entries(params).forEach(([name, value]) => {
        const $element = $(`[name="${name}"]`);

        if ($element.attr('type') === 'checkbox') {
            $element.prop('checked', value === 'on' || value === 'true');
        } else if ($element.attr('type') === 'range') {
            $element.val(value);
            const $label = $(`[for="${$element.attr('id')}"]`);
            if ($label.length) {
                $label.text(value);
            }
        } else {
            $element.val(value);
        }
    });
}

/* ---------------------------------------------------
         Miscellaneous functions
---------------------------------------------------*/
function downloadCSV(actionUrl) {
    var formData = $('form').serialize();
    var form = $('<form>', {
        action: actionUrl,
        method: 'post'
    });
    $.each(formData.split('&'), function (i, v) {
        var parts = v.split('=');
        form.append($('<input>', {
            type: 'hidden',
            name: decodeURIComponent(parts[0]),
            value: decodeURIComponent(parts[1])
        }));
    });
    form.appendTo('body').submit().remove();
}