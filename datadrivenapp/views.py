from django.shortcuts import render, HttpResponseRedirect, redirect
import pyodbc
import numpy as np
import pandas as pd
from django.urls import reverse, reverse_lazy
from django.http import HttpResponseRedirect
from django.db.utils import OperationalError
from sqlalchemy import create_engine
from django.conf import settings
import datetime as dt
from django.db.models import Q
from os.path import expanduser as ospath
from django.core.files.storage import FileSystemStorage
from django.db.models import Sum, Count
from itertools import chain
from .forms import ExtractDataENGAS
from django_datatables_view.base_datatable_view import BaseDatatableView
from django.utils.html import escape
from django.core.files.storage import FileSystemStorage
import os
import re 

import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from ftfy import fix_text
import networkx as nx


def pickleOut(obj, fname):
    fout =  open(fname, 'wb')
    pickle.dump(obj, fout)
    fout.close()
    print(f'object was successfully exported to {fname}')

def pickleIn(fname):
    fIn =  open(fname, 'rb')
    obj = pickle.load(fIn)
    fIn.close()
    print(f'object {fname} was successfully imported  ')
    return obj

def fixStrTextNorm(dstr):
    dstr = fix_text(dstr)
    dstr = dstr.lower()
    dstr = dstr.replace('|', '')
    dstr = dstr.strip()
    return dstr 

def dfObjColConverter(df, ltcols):
    for col in ltcols:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('') 
            df[col]  =  df[col].astype('str').apply(fixStrTextNorm)
    return df

def compNameGenerator(df, ltcols, sep = '| ' ):    
    return  df[ltcols].apply(lambda x: sep.join(x.dropna().astype(str).values), axis=1) 

def ngrams(string, n=3):
    string = re.sub(r'[,-./|]',r'', string)
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]

def dfMatchesTodDupID(df):
    g = nx.from_pandas_edgelist(df, 'DatabaseData', 'QueryData')
    ltconx = [{'compname': list(it)} for it in nx.connected_components(g)]
    dfgroup = pd.DataFrame(ltconx)
    dfgroup['groupID'] = dfgroup.index
    dfduptracker = dfgroup.explode('compname')
    return dfduptracker 


## DASHBOARD ##

def index(request):
    template = "datadrivenapp/dashboard.html"

    context = { 

            }

    return render(request, template, context)

## TRANSACTIONS ##

def index_trans(request):
    template = "datadrivenapp/index_trans.html"

    # sector = Agency.objects.values('SECTOR').distinct()
    # trans = Transaction.objects.values('DESCRIPTION').distinct().filter(DESCRIPTION__in = ['Disbursement', 'Collections'])
    agency = Agency.objects.all()  #agency = Agency.objects.all().filter(AGENCYCODE = 'CHED')

    context = { 
            "agency": agency
            }

    return render(request, template, context)

def data_match(request):
    template = "datadrivenapp/index_trans.html"

    context = { 
            "agency": agency
            }

    return render(request, template, context)

## EXCEL FILE UPLOAD ##

def index_setup(request):
    if request.method == 'POST' and request.FILES['fileOne'] :
        fileOne = request.FILES['fileOne']
        fs = FileSystemStorage()
        os.remove(os.getcwd()+'/media/upload.csv')
        filename = fs.save('upload.csv', fileOne)
        uploaded_file_url = fs.url(filename)
        response = redirect('/dedupe')
        return response


    template = "datadrivenapp/setup.html"

    context = { 

            }
    return render(request, template, context)



def vwDedupe(request):
    if request.method == 'POST' :
        # knn Code should be here
        df = pd.read_csv(os.getcwd()+'/media/upload.csv')
        ltColumns  = sorted(list(df.columns))
        ltColumnsToMatch  = [ key for key in request.POST.keys() if key in ltColumns]
        ltColumnsToMatch  = sorted(ltColumnsToMatch)
        df =  dfObjColConverter(df, ltColumnsToMatch)
        df['compname'] = compNameGenerator(df, ltColumnsToMatch)
        dfdb =  df.compname.drop_duplicates()
        vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
        db_tf_idf_matrix = vectorizer.fit_transform(dfdb)
        knn = NearestNeighbors(n_neighbors=10 , metric = 'cosine' )
        knn.fit(db_tf_idf_matrix)
        D, I = knn.kneighbors(db_tf_idf_matrix, 10)  

        matches = []
        for r,indVals in enumerate(I):
            for c, dbloc in enumerate(indVals):
                temp = [D[r][c], dfdb.iloc[dbloc], dfdb.iloc[r]]
                matches.append(temp)

        dfmatches = pd.DataFrame(matches, columns = ['Kdistance','DatabaseData','QueryData'])
        dfmatches = dfmatches[dfmatches['Kdistance'] > 0.000000001].sort_values('Kdistance', ascending = True)
        resfname = os.getcwd()+'/pkl/results.pkl'
        os.remove(resfname)
        pickleOut(dfmatches, resfname)
        


        response = redirect('/results')
        return response


    df = pd.read_csv(os.getcwd()+'/media/upload.csv')
    ltColumns  = sorted(list(df.columns))
    template = "datadrivenapp/dedupe.html"



    context = { 
            'ltColumns': ltColumns,
            }
    return render(request, template, context)



def vwResults(request):

    resfname = os.getcwd()+'/pkl/results.pkl'
    dfmatches = pickleIn(resfname)

    template = "datadrivenapp/results.html"
    context = { 
            'dfmatches': dfmatches,
            }
    return render(request, template, context)









def file_upload(request):
    if request.method == 'POST' and request.FILES['fileOne'] and request.FILES['fileTwo']:
        fileOne = request.FILES['fileone']
        fileTwo = request.FILES['filetwo']
        fs = FileSystemStorage()
        filenameOne = fs.save(fileOne.name, fileone)
        filenameTwo = fs.save(fileTwo.name, filetwo)
        uploaded_file_url = fs.url(filename)
        return render(request, 'datadrivenapp/view_index.html', {
            'uploaded_file_url': uploaded_file_url
            })

    template = "datadrivenapp/view_index.html"

    return render(request, template)




## REPORTS ## 

def index_view(request):
    template = "datadrivenapp/view_index.html"

    context = { 

            }

    return render(request, template, context)





'''


    context = { 

    }
    return render(request, template, context)


'''


