#!/home/hduser/anaconda/bin/python
import sys
import os
from math import log
import numpy as np

# Word to use find/use
env_vars = os.environ
min_occurr = env_vars['min_occurr']

word = []
word_count = []
ID = []
TRUTH = []

for line in sys.stdin:
    key_values = line.split('\t')
    # The key is ONE of the words we're counting
    if len(word) != 0:
        if word[-1] != key_values[0]:
            word.append(key_values[0])
    else:
        word.append(key_values[0])
    word_count.append(int(key_values[1]))
    # ID and TRUTH are replicated for each word in the vocabulary
        # so we just need to keep track of them once
    if len(word) == 1:
        ID.append(key_values[2])
        TRUTH.append(int(key_values[3]))

# The lists above will have a length equal to
    # number_different_words_in_vocab * number_emails_in_dataset
vocab_size = len(word)
num_emails = len(TRUTH)
# Reshape the list word_count into a 2-D numpy array
word_count = np.array(word_count).reshape(len(word), num_emails)
# Drop words with a frequency of less than min_ocur
condition = np.sum(word_count,1) >= int(min_occurr)
final_word_count = word_count[condition, :]
filtered_indices = np.extract(condition, word_count).tolist()
final_word = [word[i] for i in filtered_indices]
final_vocab_size = len(final_word)
                        
total_spam = sum(TRUTH) # total count of spam emails
total_ham = len(TRUTH) - total_spam # total count of ham emails
total_word_spam = [0]*len(final_word)
total_word_ham = [0]*len(final_word)
for i,w in enumerate(final_word):
    # Total count of word w in spam emails
    total_word_spam[i] = sum([x*y for (x,y) in zip(TRUTH,final_word_count[i])]) 
    # Total count of word w in ham emails
    total_word_ham[i] = sum(final_word_count[i]) - total_word_spam[i]

# PRIORS
prob_ham = float(total_ham)/(total_ham+total_spam)
prob_spam = 1 - prob_ham
# CONDITIONAL LIKELIHOODS
prob_word_ham = [float(x+1) / (sum(total_word_ham)+final_vocab_size) for x \
                 in total_word_ham]
prob_word_spam = [float(x+1) / (sum(total_word_spam)+final_vocab_size) for \
                  x in total_word_spam]

# Assess classification with the training set 
CLASS = [0]*num_emails
for i in range(num_emails): # for each email
    # POSTERIORS
    prob_ham_word = log(prob_ham,10) + \
        sum([x*log(y,10) for (x,y) in zip(final_word_count[:,i],prob_word_ham)])
    prob_spam_word = log(prob_spam,10) + \
        sum([x*log(y,10) for (x,y) in zip(final_word_count[:,i],prob_word_spam)])
    # The right side of the equations are not equal to prob_category_word, but 
        # to log(prob_category_word) - log(prob_word) (where prob_word is the 
        # EVIDENCE). It's OK since we only want to compare the POSTERIORS
    if prob_spam_word > prob_ham_word: # classify as spam if posterior is higher
        CLASS[i] = 1
    # Output for each email
    print ID[i] + '\t' + str(TRUTH[i]) + '\t' + str(CLASS[i])

# Training error
# Count of misclassification errors
errors = sum([x!=y for (x,y) in zip(TRUTH,CLASS)])
training_error = float(errors) / len(TRUTH)
# Additional line
print 'Training Error\t' + str(training_error)