import string
import os
import re
import pandas as pd
import numpy as np
from alphabet_detector import AlphabetDetector
from collections import defaultdict

# Fonction qui nettoie un peu les jobs titles
# Elle met les caractères en minuscule, enlève les chiffres et la poncutation
# Supprime les espaces supplémentaires et les caractères non latins
def preprocess(df):
    ad = AlphabetDetector()
    exclude = set(string.punctuation)
    def process_special_caracters(s):
        l=[]
        for ch in s:
            if ch not in exclude:
                l.append(ch)
            else:
                l.append(' ')
        y = ''.join(l)
        return y.lstrip()
    
    df['job_title'] = df['job_title'].apply(lambda x: x.lower())
    df['job_title'] = df['job_title'].apply(lambda x: ''.join([i for i in x if not i.isdigit()]))
    df['is_alphabet'] = df['job_title'].apply(lambda x: ad.only_alphabet_chars(str(x), "LATIN"))
    df = df[df['is_alphabet'] == True]
    df['job_title'] = df['job_title'].apply(lambda x: re.sub(' +',' ', process_special_caracters(x)))
    del df['is_alphabet']
    return df

# Fonction qui lit le fichier et calcule les statistiques demandées
# On lit le fichier grâce à Pandas et en utilisant des chunks
# C'est-à-dire que le fichier est splité et lu partie par partie
def read_and_compute_stats(input_file, index_col):
    col_names = ['job_title', 'industry', 'company']
    
    if index_col == 'industry':
        other = 'company'
    else:
        other = 'industry'
        
    chunksize = 10000
    input_data = pd.read_csv(input_file, sep='|', iterator=True, chunksize=chunksize, error_bad_lines=False, verbose=0) 

    latest = pd.DataFrame(columns=['job_title', index_col, 'total'])

    for i, chunk in enumerate(input_data):
        chunk.columns = col_names
        chunk = preprocess(chunk)
        chunk = chunk.groupby([index_col,'job_title'], as_index=False).count().rename(columns={other:'total'})
        sub_total = pd.merge(chunk, latest, how='outer', on=[index_col,'job_title'], suffixes=('_old', '_new')).fillna(0)
        sub_total['total'] = sub_total['total_old'] + sub_total['total_new'] 
        del sub_total['total_old']
        del sub_total['total_new']
        latest = sub_total
    
    # Calcule le top50 des job title par industry ou company 
    top50 = latest.groupby([index_col])['job_title', 'total'].apply(lambda x: x.nlargest(50, columns=['total']))
    top50.reset_index(inplace=True)
    del top50['level_1']
    return top50

# Fonction qui écrit les fichiers dans le format demandé
def write_file(top50, index_col):
    file_name = '{index_col}_top50.txt'.format(index_col=index_col)
    
    dic_list = top50.to_dict('records')
    dic_per_index = defaultdict(list)
    for row in dic_list:
        dic_per_index[row[index_col]].append('{job_title}:{total}'.format(job_title=row['job_title'], total=int(row['total'])))
    
    with open(file_name, 'w') as f:
        for key in iter(dic_per_index.keys()):
            f.write('{key}|{elem}\n'.format(key=key, elem=','.join(dic_per_index[key])))


def run():
    input_file = 'job_post.log'
    for index_col in ['industry', 'company']:
        print(index_col)
        top50 = read_and_compute_stats(input_file, index_col)
        write_file(top50, index_col)

if __name__ == '__main__':
    run()