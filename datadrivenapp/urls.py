from django.conf.urls import url
from django.conf import settings
from django.conf.urls.static import static

from datadrivenapp.views import *

urlpatterns = [
	url(r'^$', index, name='index'),
	url(r'^setup/$', index_setup, name='index_setup'),
	url(r'^dedupe/$', vwDedupe, name='dedupe'),
	url(r'^dpkdistance/$', vwDpKdistance, name='dpkdistance'),
	url(r'^dpresults/$', vwDpResults, name='dpresults'),
	url(r'^results/$', vwResults, name='results'),
	url(r'^view-data/$', index_view, name='index_view'),
	url(r'^transaction/$', index_trans, name='index_trans')
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
