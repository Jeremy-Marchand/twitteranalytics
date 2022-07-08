import string
import nltk

nltk.download("stopwords")
nltk.download("punkt")
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Removing numerical values
def num_remove(text):
    text_rwkd = ""
    for car in text:
        text_rwkd += car if not car.isdigit() else ""
    return text_rwkd


# Removing ponctuation
def punct_remove(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, "")
    return text


# removing stop words
def stop_remove(text):
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)
    return " ".join([w for w in word_tokens if not w in stop_words])
