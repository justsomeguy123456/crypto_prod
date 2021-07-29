import praw
import pprint

import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

with open('../reddit.txt', 'r') as fp:
    lines = fp.readlines()



reddit = praw.Reddit(
    client_id=lines[0].strip(),
    client_secret=lines[1].strip(),
    password=lines[2].strip(),
    user_agent=lines[3].strip(),
    username=lines[4].strip(),
)




#print(reddit.user.me())
#print(reddit.read_only)

subs = ['Ethereum','bitcoin','CryptoCurrency']



result = {'sub':[],
          'pos':[],
          'neg':[],
          'neu':[],
          'pos%':[],
          'neg%':[],
          'neu%':[],
          'bull':[],
          'bear':[],
          'btc':[],
          'eth':[],
          'total_comments':[]}

for s in subs:

    subreddit = reddit.subreddit(s)

    result['sub'].append(s)
    submis_dict = {'title':[],
                   'id':[],
                   'date':[]
                    }

    comment_dict = {'author':[],
                    'post_id':[],
                    'body':[],
                    'date':[],
                    'comment_id':[]
                    }
    total_comments = 0
    for submission in subreddit.new(limit=6):
        #pprint.pprint(vars(submission))
        #print(str(submission.title),'-----', submission.id, submission.created_utc, str(submission.stickied))
        if (str(submission.stickied) != 'True') or (str(submission.title).strip() == 'Weekly Discussion Thread') or (str(submission.title).find('Daily Discussion') < -1):
            #print(submission.title,'-----', submission.id, submission.created_utc, submission.stickied)
            submis_dict['title'].append(submission.title)
            submis_dict['id'].append(submission.id)
            submis_dict['date'].append(submission.created_utc)
            #print('comments')
            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():
                    #print('*****')
                    comment_dict['author'].append(comment.author)
                    comment_dict['post_id'].append(comment.link_id)
                    comment_dict['body'].append(comment.body)
                    comment_dict['date'].append(comment.created_utc)
                    comment_dict['comment_id'].append(comment.id)
                    total_comments +=1
            #print('comment   :' ,comment.author, comment.id, comment.link_id, comment.body, comment.created_utc)


        #print('*************************')

    sia = SentimentIntensityAnalyzer()

    title_st =''
    coment_st = ''
    stopwords = nltk.corpus.stopwords.words("english")

    for r in submis_dict['title']:
        #print(r)
        title_st += r

    #print(title_st)

    pos = 0
    neg = 0
    neu = 0

    for  r in comment_dict['body']:
        #print(r)
        #print(sia.polarity_scores(r))
        p_s = sia.polarity_scores(r)
        if p_s['compound'] >0.05:
            pos += 1
        elif p_s['compound'] < -0.05:
            neg += 1
        else:
            neu +=1



        #print(p_s)
        coment_st += r.lower()
    result['pos'].append(pos)
    result['neg'].append(neg)
    result['neu'].append(neu)
    #print(coment_st)
    coment_word_list = []
    #coment_word_list = [w for w in nltk.word_tokenize(coment_st) if w.isalpha()]

    for w in nltk.word_tokenize(coment_st):
        if w.isalpha() == True: #and w not in stopwords:
            coment_word_list.append(w)


    #coment_word_list = [w for w in coment_st if w not in stopwords]
    #print(coment_word_list)
    fd = nltk.FreqDist(coment_word_list)
    finder = nltk.collocations.QuadgramCollocationFinder.from_words(coment_word_list)
    finder.apply_word_filter(lambda w: w in ('bot','auto'))

    pos_per = 0.000
    neg_per = 0.000
    neu_per = 0.000
    if total_comments != 0:

        pos_per = float(pos) / float(total_comments)
        neg_per = float(neg) / float(total_comments)
        neu_per = float(neu) / float(total_comments)

    result['pos%'].append(pos_per)
    result['neg%'].append(neg_per)
    result['neu%'].append(neu_per)
        #result['pos%'][0] =float(result['pos'][0]) / float(total_comments)
        #result['neg%'][0] = float(result['neg'][0])  / float(total_comments)
        #result['neu%'][0] = float(result['neu'][0]) / float(total_comments)


    words= ['bull', 'bullish','bulls','bear','bearish','bears','eth','btc','bitcoin','ethereum','network']

    bull = 0
    bear = 0
    btc = 0
    eth = 0
    for w in words:
        #print(w,fd[w])
        if w in ['bull', 'bullish','bulls']:
            bull += fd[w]
        if w in ['bear','bearish','bears']:
            bear += fd[w]
        if w in ['eth','ethereum']:
            eth += fd[w]
        if w in ['btc','bitcoin']:
            btc += fd[w]
    result['bull'].append(bull)
    result['bear'].append(bear)
    result['eth'].append(eth)
    result['btc'].append(btc)
    #print(coment_word_list)
    result['total_comments'].append(total_comments)
    print(result)
    print(total_comments)
    #print(finder.ngram_fd.most_common(10))
#print(word_tokenize(coment_st))

result_df = pd.DataFrame.from_dict(result)

    # = result_df.append(df)

result_df.to_excel('../reddit_comments.xlsx')
