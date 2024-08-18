# TAIC report analysis

![Website](https://img.shields.io/website?url=https://taic-document-searcher-cfdkgxgnc3bxgbeg.australiaeast-01.azurewebsites.net/login&up_message=live&down_message=not%20available&label=production%20webapp&link=https://taic-document-searcher-cfdkgxgnc3bxgbeg.australiaeast-01.azurewebsites.net/)
[![tests](https://github.com/1jamesthompson1/TAIC-report-summary/actions/workflows/ci.yml/badge.svg)](https://github.com/1jamesthompson1/TAIC-report-summary/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/1jamesthompson1/TAIC-report-summary/graph/badge.svg?token=3IMJCA4B49)](https://codecov.io/gh/1jamesthompson1/TAIC-report-summary)


***

#### _A project that has built a tool and webapp to support both investigators and researchers in understanding the vast swath of previous investigations._

## What

This GitHub repo is a project repository that has been built throughout [my](https://github.com/1jamesthompson1) experience and efforts of applying data science solutions to transport accident investigations.

There are fundamentally two deliverables in this project.  
1. [engine](#engine) which is a Python program that gathers publicly available investigation reports and extracts/infers information to make datasets of safety issues, recommendations etc.  
2. [viewer](#viewer) which is a Python Flask webapp deployed with Azure that implements a [RAG](https://arxiv.org/abs/2005.11401v4) search engine to search through some of the datasets generated in the engine.

There is also a [collection](https://github.com/1jamesthompson1/TAIC-report-summary/tree/d735c0f3a50f4ef24f1e7198730c984fdb3446c7/notebooks) of jupyter notebooks that show the development process of some of the more complex features in the engine and viewer.


### About

This project started as a university project for my final semester. The university work was completed in July-October 2023 and finished with a basic engine and viewer app. Since then work has been completed directly with TAIC to bring the engine and viewer from POC -> Prototype -> Production. It should be released internally sometime in October 2024. 

### More information

If you want more information about the project and how it works/how you can use it you should checkout the [wiki](https://github.com/1jamesthompson1/TAIC-report-summary/wiki).

However if you are curious about the project and its history and non-technical documentation you can check out the [Project documents](https://github.com/1jamesthompson1/TAIC-report-summary/tree/main/Project%20documents)