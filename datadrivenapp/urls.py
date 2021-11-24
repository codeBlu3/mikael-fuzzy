from django.conf.urls import url
from django.conf import settings
from django.conf.urls.static import static

from datadrivenapp.views import *

urlpatterns = [
	## DASHBOARD URLS ##
	url(r'^$', index, name='index'),
	## SETUP URLS ##
	url(r'^setup/$', index_setup, name='index_setup'),
	## VIEWING URLS ##
	url(r'^view-data/$', index_view, name='index_view'),
	## TRANSACTIONS URLS ##
	url(r'^transaction/$', index_trans, name='index_trans')
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
