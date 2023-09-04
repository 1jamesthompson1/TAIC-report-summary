# This is simply so that i can use poetry script to run the shiny app

import os

def run():
    os.system("shiny run --reload viewer/app.py")