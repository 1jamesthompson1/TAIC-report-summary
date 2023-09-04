from shiny import App, render, ui, reactive
import pandas as pd

import search

app_ui = ui.page_fluid(
    ui.h1("TAIC Legacy Report Viewer"),
    ui.input_text("searchQuery", "Search Query", placeholder="Enter search query"),
    ui.input_action_button("showReports", "Show Reports"),
    ui.output_table("reports_table"),
)


def server(input, output, session):
    @output
    @render.table
    async def reports_table():
        input.showReports()        # Take a dependency on the button
        input.searchQuery()        # Take a dependency on the search query
        
        searcher = search.Searcher()


        with reactive.isolate():
            return searcher.search(input.searchQuery())


app = App(app_ui, server)
