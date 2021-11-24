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
		filename = fs.save('upload.csv', fileOne)
		uploaded_file_url = fs.url(filename)
		return render(request, 'datadrivenapp/setup.html', {
		    'uploaded_file_url': uploaded_file_url
		})
  

	template = "datadrivenapp/setup.html"

	context = { 
		 
	}
	return render(request, template, context)

def render_result(request):
    df = pd.read_csv('upload.csv')
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

    dfmatches = pd.DataFrame(matches, columns = ['Kdistance','DatabaseData','QueryData'])\
                .sort_values('Kdistance', ascending = True)
    return dfmatches






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


