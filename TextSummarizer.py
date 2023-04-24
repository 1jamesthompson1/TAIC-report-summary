import os
import nltk
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.cluster.util import cosine_distance
import numpy as np
import heapq

nltk.download('stopwords')
nltk.download('punkt')

def summarizeText(input_text, num_sentences):
    stop_words = set(stopwords.words('english'))
    words = word_tokenize(input_text.lower())
    words = [PorterStemmer().stem(w) for w in words if w.isalpha()]
    freq = FreqDist(words)
    max_freq = max(freq.values())
    for w in freq.keys():
        freq[w] = (freq[w]/max_freq)
    sent_list = sent_tokenize(input_text)
    sent_scores = {}
    for sent in sent_list:
        for word in nltk.word_tokenize(sent.lower()):
            if word in freq.keys():
                if len(sent.split(' ')) < 30:
                    if sent not in sent_scores.keys():
                        sent_scores[sent] = freq[word]
                    else:
                        sent_scores[sent] += freq[word]
    best_sentences = heapq.nlargest(num_sentences, sent_scores, key=sent_scores.get)
    return '\n'.join(best_sentences)

def summarizeFiles(input_folder, output_folder, num_sentences):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for filename in os.listdir(input_folder):
        if filename.endswith('.txt'):
            with open(os.path.join(input_folder, filename), 'r') as f:
                input_text = f.read()
                if len(input_text) < 100:
                    continue
                summary = summarizeText(input_text, num_sentences)
                with open(os.path.join(output_folder, filename.replace('.txt', '_summary.txt')), 'w', encoding='utf-8') as summary_file:
                    summary_file.write(summary)
            print(f'Summarized {filename} and saved summary to {os.path.join(output_folder, filename.replace(".txt", "_summary.txt"))}')

# input_folder = 'extracted_text'
# output_folder = 'summarised'
# num_sentences = 5
# summarizeFiles(input_folder, output_folder, num_sentences)