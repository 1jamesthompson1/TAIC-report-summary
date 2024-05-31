---
title: "State and possible future direction"
subtitle: "AI at TAIC"
author: "James Thompson - 1jamesthompson1@gmail.com"
date: "7th May 2024"
pdf_document: null
geometry: margin=1.8cm
output: pdf_document
---

# Introduction

The focus of the project is to develop a pipeline of processing and analysis for TAIC's reports that could be implemented at other agencies. Currently the project's areas can be split into three categories. These are "Metadata extraction and dataset building", "Searching the dataset" and "Data mining of the dataset". The first chunk of work that was completed prior to Christmas 2023 was focussed on metadata extraction with a little work on the searching problem. Recent work has developed and hardened metadata extraction. The project is being worked on publicly on GitHub [here](https://github.com/1jamesthompson1/TAIC-report-summary)

# State

The current project takes in reports and extracts the metadata namely, safety issues, recommendations etc to give us a dataset. There has been some analysis of the dataset as well as a trivial implementation of searching this dataset with the web viewer.

The last couple months of work has been expanding the metadata extraction and increasing its trustworthiness. This metadata extraction pipeline is substantially completed for TAIC and now work can be done making this dataset as useful as possible.

# Avenues of future work

Each one of these avenues can be pursued more or less independently. There are a lot of avenues that could be pursued however these seem from my perspective to provide the most utility for TAIC.

## Metadata extraction on other agencies

The work on the engine has not been exclusively specific to TAIC's reports. This means that performing the same metadata extraction on other reports will just involve two things.

- Developing a new agency specific parser that will get reports ready to be processed
- Ironing out issues with the engine to make it robust enough to handle other reports.

I anticipate each agency will take 3 weeks to add metadata extraction for. However later ones should become quicker as more robust tools are built within the engine.

## Developing smart search capabilities of the dataset

As the number of reports in the dataset increase the ability to search and find relevant reports will be more and more important. There are two new technologies which can be really useful here. Both of the mentioned solutions are designed to work at great scale so adding in extra reports from other agencies is a straight forward problem.
Firstly is a vector database, these allow for really fast semantic searching of reports (for example a search "Boats that sunk due to fire" could return top 5 most relevant reports). (2 weeks for a prototype)
Secondly is a Retrieval Augmentation Generation system which allows you to converse with your dataset. This works by having a LLM (similar to ChatGPT) be your interface and it interacts with the vector database directly, giving it the ability to answer quite specific questions like "What is a summary of the safety issues from Boats that sunk due to a fire". (2 weeks for a prototype built on top of the vector database)

## Data mining the datasets

The process of Data mining is looking through datasets and trying to discover useful analysis that can be done. Spending time doing the data mining would come up with different ways to get useful insights. However for now I will give a few concrete examples of analysis that can be conducted. Both of these ideas are inspired by an [Australian's research into Rail Accident report safety hazards and recommendations](https://australasiantransportresearchforum.org.au/wp-content/uploads/2023/12/ATRF_2023_Paper_19.pdf)

### Generation of Safety themes from Safety issues

A little bit of work has been conducted to try and generate safety themes from safety issues. This involves using the topic model [BERTopic](https://maartengr.github.io/BERTopic/index.html) and produces themes from all of the safety issues. This is yet to be completed but does pose an interesting replacement/supplement of TAIC's watchlist.

![Example topic model of safety issues and generated safety themes](example_model.png){ width=650px }


### Modelling recommendations

A recommendation dataset also exists for TAIC which allows for a variety of analysis. Firstly similar to what was done with the safety themes you could generate recommendation themes. Alternatively you could build a different type of topic model to help determine what level of the system is being targeted (oversight, single business etc) by a recommendation. These experiments would take 3-4 weeks to fully investigate.

# Conclusion

In its current state the project could be polished off and made into a production level simple searcher of TAIC's extracted dataset rather easily. However this would miss out on the great potential on offer. By instead polishing off the complete extract -> search -> analysis pipeline, there will be a model that can be implemented across other agencies. It will take a bit over a month of full time work to do this polishing then another month to add in another agencies reports. Each successive agency should be easier to add into the scope.