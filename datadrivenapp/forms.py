from django import forms
from django.forms.widgets import SelectDateWidget
from django.utils import timezone

class ExtractDataENGAS(forms.Form):
	AGENCY = forms.CharField(label='Agency', max_length=100)
	DATE_FROM = forms.DateTimeField(
        input_formats=['%d/%m/%Y'],
        widget=forms.DateTimeInput(attrs={
            'class': 'form-control datetimepicker-input',
            'data-target': '#datetimepicker1'
        })
    )
	DATE_TO = forms.DateField(widget=forms.SelectDateWidget(), initial=timezone.now())
