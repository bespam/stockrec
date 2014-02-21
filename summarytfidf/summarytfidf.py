import json
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer
from sklearn.metrics.pairwise import linear_kernel
import pdb
import sys
from scipy.linalg import svd
from matplotlib import pyplot as plt
import numpy as np
import pickle
import os


def analysis(stocks, corpus_reload, n_features, min_df,stock_rec):
    #directory of the current module
    module_dir = os.path.dirname(__file__)
    if module_dir != "": module_dir = module_dir + "\\"
    #initialize countVectorizer and tfidfTransformer
    vectorizer = CountVectorizer(min_df = min_df)
    transformer = TfidfTransformer(norm="l2")
    #list of all keywords
    corpora = []
    stocks_finite = []
    print("total stocks: "+ str(len(stocks)))
    if corpus_reload == True or os.path.isfile(module_dir + 'corpora.pickle') == False:
        i = -1
        for tick in stocks:
            i = i + 1
            print("\r"+ str(i)),
            #initialize Lemmatizer
            lmtzr = WordNetLemmatizer()
            summary = profile[tick]["businessSummary"].encode("ascii","xmlcharrefreplace")
            if summary == "N/A":
                continue
            else:
                # sent tokenize
                sentences = sent_tokenize(summary)
                words = []
                for sent in sentences:
                    words.extend(word_tokenize(sent))
                #remove stopwords
                words_nonstop = [w.lower() for w in words if w not in stopwords.words("english")]
                #Lemmatize
                words_lmtzr = [lmtzr.lemmatize(w) for w in words_nonstop]        
                pos = nltk.pos_tag(words_lmtzr)
                keywords = [p[0] for p in pos if p[1] in ['NN','NNP','NNS']]
            
            corpora.append(" ".join(keywords))
            stocks_finite.append(tick)
        #dump corpora
        f = open(module_dir+"corpora.pickle","wb")
        pickle.dump(corpora,f)
        f = open(module_dir+"corpora_stocks.pickle","wb")
        pickle.dump(stocks_finite,f)
    else:
        f = open(module_dir+"corpora.pickle","rb")
        corpora = pickle.load(f)
        f = open(module_dir+"corpora_stocks.pickle","rb")
        stocks_finite = pickle.load(f)
    
    print("calculating tfidf")
    vectorizer.fit_transform(corpora)
    freq_term_matrix = vectorizer.transform(corpora)
    transformer.fit(freq_term_matrix)
    tfidf = transformer.transform(freq_term_matrix)
    
    print("performing SVD analysis to extract primary components")    
    u,s,v = svd(tfidf.todense().T)
    #cut v (number of features to retain)
    v_cut = v[:n_features,:].T
    
    #renormalize v_cut for mod(vect) = 1
    for i in range(len(stocks_finite)):
        v_cut[i,:] = v_cut[i,:]/np.sqrt(v_cut[i,:].dot(v_cut[i,:]))
    
    
    # save matrix to summary_tfidf.json
    summary_tfidf = {}
    i = 0
    print("total stocks: "+ str(len(stocks)))
    for stock in stocks:
        print("\r"+str(i)+ " stocks were saved"),
        if stock in stocks_finite:
            summary_tfidf[stock] = v_cut[i,:].flatten().tolist()
            i += 1
        else:
            summary_tfidf[stock] = None
    print("ok")
    file = module_dir+"summary_tfidf.json"
    f = open(file,"w") 
    json.dump(summary_tfidf,f)
    
    if len(stock_rec) > 0:
        stock_index = stocks_finite.index(stock_rec)    
        stocks_finite = np.array(stocks_finite)
        cosine_similarities = linear_kernel(tfidf[stock_index], tfidf).flatten()
        indx = cosine_similarities.argsort()[:-10:-1]
        print stock_rec, stocks_finite[indx],cosine_similarities[indx] 
        
        cosine_similarities_2 = linear_kernel(v_cut[stock_index], v_cut).flatten()
        indx2 = cosine_similarities_2.argsort()[:-10:-1]
        print v_cut[stock_index]
        print stock_rec, stocks_finite[indx2],cosine_similarities_2[indx2]/cosine_similarities_2[indx2[0]]



    #plt.plot(s)
    #plt.show()
    #pdb.set_trace()
        


if __name__ == '__main__':
    if len(sys.argv) > 1:
        arg = sys.argv[1]
    else:
        arg = "skip"
    dataDir = "..\\data\\"
    # reading profile file
    file = dataDir + "stocks_profiles.json"
    f = open(file,"r") 
    profile = json.load(f)
    stocks = profile.keys()
    analysis(stocks, False,20,20,"GOOG")