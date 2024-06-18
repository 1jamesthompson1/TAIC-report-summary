# TAIC report analysis

![Website](https://img.shields.io/website?url=https%3A%2F%2Ftaic-viewer-72e8675c1c03.herokuapp.com%2F&up_message=live&down_message=not%20available&label=webapp%20demo&link=https%3A%2F%2Ftaic-viewer-72e8675c1c03.herokuapp.com%2F)


***

#### _A tool and webapp to support both investigators and researchers in understanding the collection of previous investigations_

## What

This is a Semantic Analysis project to look at the marine investigation reports made by the Transport accident investigation commission ([TAIC](https://www.taic.org.nz/)) in New Zealand.
There is only use of publicly available data and the reports will be summarised using a variety of techniques. The reports that will be looked can be found on TAIC's [website](https://www.taic.org.nz/inquiries?order=field_publication_date_value&sort=desc&keyword=&date_filter%5Bmin%5D%5Bdate%5D=&date_filter%5Bmax%5D%5Bdate%5D=&publication_date%5Bmin%5D%5Bdate%5D=&publication_date%5Bmax%5D%5Bdate%5D=&status%5B0%5D=12).

The webapp demo can be found at https://taic-viewer-72e8675c1c03.herokuapp.com/. Information on how to use the webapp is found in the [wiki](https://github.com/1jamesthompson1/TAIC-report-summary/wiki/How-you-can-use-this-program#webapp-user-instructions). The core of the project however is a python program that uses LLMs to read and provide summaries/analysis of the accident reports. More information about how the project works can be found [here](https://github.com/1jamesthompson1/TAIC-report-summary/wiki/How-this-project-works).

## When

This project was commenced as my final year Computer Science project for my BSc at Massey University. Further work has been completed with TAIC in November/December 2023 with more work expected to commence in March 2024.

## More information

The wiki for this project is being developed here https://github.com/1jamesthompson1/TAIC-report-summary/wiki. There is substantionally more infromation there as well as in the [Project documents folder](https://github.com/1jamesthompson1/TAIC-report-summary/tree/f5742e344ad97b8b97b7e9dc96788e092a637233/Project%20documents).

### Rubbish bin of interesting ideas

To keep the repo clean I have tried to be quite onto it in delete unused modules. However this does mean that in some cases I have deleted good work that might be useful at another point. It is worth checking here to see if your idea has been looked at before as this could give a jumpstart.

Here is a (non-exhaustive) list:
| deleted item | description |
|--------------|-------------|
| [ReferenceChecking](https://github.com/1jamesthompson1/TAIC-report-summary/blob/bdd9445670c3a1ec659cad02b4eb91e200ff10cb/engine/Extract_Analyze/ReferenceChecking.py) | This is a module that checks where the references made within a text are true and reasonable. It was removed when the output of the model no longer had references in it because `Summary` was deleted. |
| [ThemeGeneration](https://github.com/1jamesthompson1/TAIC-report-summary/blob/bdd9445670c3a1ec659cad02b4eb91e200ff10cb/engine/Extract_Analyze/ThemeGenerator.py) | This module reads all of the reports and generates some safety themes. This method was deprecated in replace of [topic modelling](https://github.com/1jamesthompson1/TAIC-report-summary/issues/144) |
| [Summarizer](https://github.com/1jamesthompson1/TAIC-report-summary/blob/bdd9445670c3a1ec659cad02b4eb91e200ff10cb/engine/Extract_Analyze/Summarizer.py) | Summarizer main task was to go through all of the reports and assign a weight to how much each safety theme related to the report. As safety themes are no longer generated the same this module was removed.