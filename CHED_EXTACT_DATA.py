#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import pyodbc
#import openpyxl
import re
import string
import math
import sqlalchemy
import urllib


# In[2]:


server = 'DESKTOP-7UQLR6M'
db = 'ENGAS_CHED'
# uid = 'ca_rlc'
# pword = 'ITAOCArlc01'

conn = pyodbc.connect('Driver={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';')


# #### Load eNGAS

# In[3]:


sql_engas = '''select T131.TRANSACTION_NO, 
    T131.JEV_NO, T131.PARTICULARS, LEFT(T131.PARTICULARS, 90) PARTICULARS2, T131.ENTRY_DATE,T133.AMOUNT,
    SUBSTRING(CHK.REFERENCE_DOCUMENT_VALUE , PATINDEX('%[^000]%', CHK.REFERENCE_DOCUMENT_VALUE +'.'), 
        LEN(CHK.REFERENCE_DOCUMENT_VALUE )) as CHECK_NO,  
    ADA.ADA_NO AS ADA_NO, R105.DESCRIPTION as [REFERENCE], R113.DESCRIPTION as [SUBSIDIARY_ACCOUNT], 
    R102.DESCRIPTION as [MAJOR_ACCOUNT], R108.DESCRIPTION as [TRANSACTION_TYPE], T131.YEAR_ENTRY, T132.ACCOUNT_FLAG
    from BK_T_TRANSACTION_131 T131 
    left join BK_T_TRANSACTION_DETAIL_132 T132 on T131.TRANSACTION_NO = T132.TRANSACTION_NO 
    left join BK_T_TRANSACTION_SUBSIDIARY_133 T133 on T132.TRANSACTION_NO = T133.TRANSACTION_NO and T132.TRANSACTION_DETAIL_NO = T133.TRANSACTION_DETAIL_NO 
    left join BK_R_MAJOR_ACCOUNT_102 R102 on T132.ACCOUNT_UID = R102.ACCOUNT_UID 
    left join BK_R_ACCOUNT_TYPE_101 R101 on R102.ACCOUNT_TYPE_UID = R101.ACCOUNT_TYPE_UID 
    left join BK_R_TRANSACTION_TYPE_108 R108 on T131.TRANSACTION_TYPE_UID=R108.TRANSACTION_TYPE_UID 
    left join BK_R_SUBSIDIARY_ACCOUNT_113    R113 on T133.SUBSIDIARY_UID = R113.SUBSIDIARY_UID 
    left join BK_T_TRANSACTION_REFERENCE_134 T134 on T132.TRANSACTION_NO = T134.TRANSACTION_NO 
    left join BK_R_REFERENCE_DOCUMENT_105 R105 on T134.REFERENCE_DOCUMENT_UID=R105.REFERENCE_DOCUMENT_UID
    left join (select T134.TRANSACTION_NO, T134.REFERENCE_DOCUMENT_VALUE from  BK_T_TRANSACTION_REFERENCE_134 T134
                    left join BK_R_REFERENCE_DOCUMENT_105 R105 on T134.REFERENCE_DOCUMENT_UID=R105.REFERENCE_DOCUMENT_UID
                    where R105.DESCRIPTION like '%Check%' and REFERENCE_DOCUMENT_FLAG = 1 ) CHK on T131.TRANSACTION_NO = CHK.TRANSACTION_NO
    LEFT JOIN (select T134.TRANSACTION_NO, T134.REFERENCE_DOCUMENT_VALUE AS ADA_NO from  BK_T_TRANSACTION_REFERENCE_134 T134 
                    left join BK_R_REFERENCE_DOCUMENT_105 R105 on T134.REFERENCE_DOCUMENT_UID=R105.REFERENCE_DOCUMENT_UID
                    where R105.DESCRIPTION  LIKE '%ada%') ADA ON T131.TRANSACTION_NO = ADA.TRANSACTION_NO
        WHERE APPROVED_BY is not null and CANCELLED_BY is NULL
        and R108.DESCRIPTION in ('Disbursement')
        and R105.DESCRIPTION in ('Check', 'Advice to Debit the Account (ADA)')
        and R102.DESCRIPTION in ('Cash in Bank - Local Currency, Current Account', 
    'Cash - Modified Disbursement System (MDS), Regular', 'Cash - Modified Disbursement System (MDS), Trust ')
     order by T131.TRANSACTION_NO'''

rows2 = pd.read_sql(sql_engas, conn)
# rows2 = pd.read_excel(r'D:\Python\engas_ched.xlsx')
df_engas =  pd.DataFrame(rows2)
len(df_engas)


# In[4]:


df_engas.drop_duplicates(keep = "first", inplace = True)
len(df_engas) #20673


# In[5]:


#export_excel = df_engas.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_raw.xlsx', index = None, header=True)


# In[6]:


df_engas.head()


# #### Clean-up Check_No and ADA_No

# In[7]:


df_engas['CHECK_NO'] = df_engas['CHECK_NO'].str.extract(r'(\d{5,10})')
df_engas['ADA_NO'] = df_engas['ADA_NO'].str.replace(r'([LDAP\-\s]{0,7}ADA{0,1}[NOonm\.\s]{0,5})','')
df_engas['ADA_NO'] = df_engas['ADA_NO'].str.replace(r'(^[LDAP\-\s]{0,7}[No.]{0,3})','')
df_engas['ADA_NO'] = df_engas['ADA_NO'].str.replace(r'(^[\s]*)','')
df_engas['ADA_NO'] = df_engas['ADA_NO'].str.replace(r'([a-zA-Zto\s\-]{0,8}$)','')
df_engas['ADA_NO'] = df_engas['ADA_NO'].str.replace(r'(^[a-zV-]{0,10})','' )
df_engas['ADA_NO'] = df_engas['ADA_NO'].apply(lambda x : x if len(str(x)) > 3 else np.nan) 


# #### Extract Check No from Particulars

# In[8]:


df_engas_noCheck = df_engas[df_engas['CHECK_NO'].isnull()].copy()
df_engas_Check = df_engas[df_engas['CHECK_NO'].notnull()].copy()
df_engas_noCheck['CHECK_NO'] = df_engas_noCheck['PARTICULARS'].str.extract(r'(^\d{5,10})')
df_engas_noCheck['CHECK_NO'] = df_engas_noCheck['CHECK_NO'].str.replace(r'(^[0]{1,4})','')


# In[9]:


# Join dataframe
df_engas  = pd.concat([df_engas_Check, df_engas_noCheck]) 


# In[10]:


len(df_engas)


# In[11]:


# Pad check 99 series checkdf_engas[df_engas['CHECK_NO'] less than 10 digits
df_engas['CHECK_NO2'] = df_engas['CHECK_NO'].str.extract(r'(9900[\d]{4}$)')
df_engas['CHECK_NO2'] = df_engas['CHECK_NO2'].str.replace('9900','990000')


# In[12]:


df_engas_noCheck2 = df_engas[df_engas['CHECK_NO2'].isnull()].copy()
df_engas_Check2 = df_engas[df_engas['CHECK_NO2'].notnull()].copy()

df_engas_Check2['CHECK_NO'] = df_engas_Check2['CHECK_NO2']  


# In[13]:


df_engas = pd.concat([df_engas_noCheck2, df_engas_Check2]) 


# In[14]:


df_engas.drop(['CHECK_NO2'],  axis = 1, inplace = True)
len(df_engas)


# ###  Extract ADA NO from Particulars

# In[15]:


# separte records without ADA
df_engas_noADA = df_engas[df_engas['ADA_NO'].isnull()].copy()
df_engas_withADA = df_engas[df_engas['ADA_NO'].notnull()].copy()

# for records without ADA reference, extract ADA from particulars 
str1 = "(\d{3}-\d{2}-\d{2,3}[-\s]{0,1}\d{4})"
df_engas_noADA['ADA_NO'] = df_engas_noADA['PARTICULARS2'].str.extract(str1)
#df_engas1['ADA_NO2'] = df_engas1["PARTICULARS2"].str.extract(r'(L{1,2}D{1,2}AP-{0,1}\s{0,1}ADA\s[NOo.# ]{0,4}\d{3}[-]\d{2}[-]\d{3}[-]\d{4})')


# In[16]:


# Join dataframe
df_engas  = pd.concat([df_engas_withADA, df_engas_noADA]) 


# In[17]:


#export_excel = df_engas.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_ADA.xlsx', index = None, header=True)


# ### Extract Payee from Particulars

# In[18]:


# Extract1
str_payee= '(CHED\sPAYROLL\s[FUND\s]{0,5}ACCOUNT)'
df_engas['PAYEE'] = df_engas['PARTICULARS'].str.extract(str_payee)
#df_engas_noPayee4['PAYEE'] = df_engas_noPayee4['PAYEE'].str.replace(r'(\d{5,10}\s{0,2}[A-Z]{0,1}\s{0,2}\-\s{0,2})','')


# In[19]:


#separate records with Payee
df_engas_noPayee = df_engas[df_engas['PAYEE'].isnull()].copy()
df_engas_Payee = df_engas[df_engas['PAYEE'].notnull()].copy()

# Extract2 ", 2016" or ", 2018"
str_payee = "([,.]{1}\s*2[016789]{3}\s*-*\s*[a-zA-Z24Ññ\'.\-\",]{1,15}\.{0,1}\s{0,1}[a-zA-ZÑñ10]{1,15}\.{0,1}\s{0,1}[a-zA-ZÑñ.]{0,15}\s{0,1}[a-zA-ZÑñ.]{0,15}\s{0,1}[a-zA-ZÑñ.]{0,15}\s{0,1}[a-zA-ZÑñ.]{0,15})"
df_engas_noPayee['PAYEE'] = df_engas_noPayee['PARTICULARS2'].str.extract(str_payee)
df_engas_noPayee['PAYEE'] = df_engas_noPayee['PAYEE'].str.replace(r'([,.]{1}\s*2[016789]{3}\s*-*\s*)','')
df_engas_noPayee['PAYEE'] = df_engas_noPayee['PAYEE'].str.upper()
df_engas_noPayee['PAYEE'] = df_engas_noPayee['PAYEE'].str.replace(r'(\sET[\s.AL.]{0,5}$)','') 
df_engas_noPayee['PAYEE'] = df_engas_noPayee['PAYEE'].str.replace(r'(^[\.1]{1,2})','') 


# In[20]:


#separate2 records with Payee
df_engas_noPayee2 = df_engas_noPayee[df_engas_noPayee['PAYEE'].isnull()].copy()
df_engas_Payee2 = df_engas_noPayee[df_engas_noPayee['PAYEE'].notnull()].copy()

# Extract2
str_payee2 = "(\-\s{0,1}[iA-Z4\.\-\,]{2,15}\s{0,2}[A-ZÑñ\.\-\']{1,15}\.{0,1}\s{0,2}[A-ZÑñ]{1,15}\.{0,1}\s{0,1}[A-ZÑñ]{1,15}\.{0,1}\s{0,1}[A-ZÑñ]{1,15}\.{0,1})"
df_engas_noPayee2['PAYEE'] = df_engas_noPayee2['PARTICULARS2'].str.extract(str_payee2)
df_engas_noPayee2['PAYEE'] = df_engas_noPayee2['PAYEE'].str.replace(r'(^[\-\s]{0,2})', '')
df_engas_noPayee2['PAYEE'] = df_engas_noPayee2['PAYEE'].str.replace(r'(\sET[\s.AL.]{0,5})','')
df_engas_noPayee2['PAYEE'] = df_engas_noPayee2['PAYEE'].str.replace(r'(ET.AL.)','')
df_engas_noPayee2['PAYEE'] = df_engas_noPayee2['PAYEE'].str.replace(r'(^([TO]{0,2}\s*PAYMENT[A-Z\s]{3,40}))', '')


# In[21]:


#separate3 records with Payee
df_engas_noPayee3 = df_engas_noPayee2[df_engas_noPayee2['PAYEE'].isnull()].copy()
df_engas_Payee3 = df_engas_noPayee2[df_engas_noPayee2['PAYEE'].notnull()].copy()

# Extract4
str_payee4 = '(\d{5,10}[A-Z]{0,1}\s{0,2}[A-ZÑ\.,\']{3,15}\s{0,2}[A-ZÑ\.]{0,15}\s{0,1}[A-Z\.]{0,15}\s{0,1}[A-ZÑ\.]{0,15}\s{0,1}[A-ZÑ]{0,15}\s{0,1}[A-Z]{0,15})'
df_engas_noPayee3['PAYEE'] =  df_engas_noPayee3['PARTICULARS2'].str.extract(str_payee4)
df_engas_noPayee3['PAYEE'] =  df_engas_noPayee3['PAYEE'].str.replace(r'(\d{5,10}[A-Z]{0,1}\s{0,2})','') 
df_engas_noPayee3['PAYEE'] =  df_engas_noPayee3['PAYEE'].str.replace(r'(\s[A-Z]{1}$)','')


# In[22]:


#separate4 records with Payee
df_engas_noPayee4 = df_engas_noPayee3[df_engas_noPayee3['PAYEE'].isnull()].copy()
df_engas_Payee4 = df_engas_noPayee3[df_engas_noPayee3['PAYEE'].notnull()].copy()


# Extract5
str_payee5 = '(\d{5,10}\s{0,2}[A-Z]{0,1}\s{0,2}\-\s{0,2}[A-ZÑ\.,]{2,15}\s{0,2}[A-ZÑ\.]{0,15}\s{0,1}[A-Z\.]{0,15}\s{0,1}[A-ZÑ\.]{0,15}\s{0,1}[A-ZÑ]{0,15}\s{0,1}[A-Z]{0,15})'
df_engas_noPayee4['PAYEE'] = df_engas_noPayee4['PARTICULARS2'].str.extract(str_payee5)
df_engas_noPayee4['PAYEE'] = df_engas_noPayee4['PAYEE'].str.replace(r'(\d{5,10}\s{0,2}[A-Z]{0,1}\s{0,2}\-\s{0,2})','')


# In[23]:


#separate5 records with Payee
df_engas_noPayee5 = df_engas_noPayee4[df_engas_noPayee4['PAYEE'].isnull()].copy()
df_engas_Payee5 = df_engas_noPayee4[df_engas_noPayee4['PAYEE'].notnull()].copy()

# Extract6
xstr_payee6 = '(dtd\s\d{2}-\d{2}-\d{2}[\s\-]{0,3}[a-zA-ZÑ\.,]{1,15}\s{0,1}[a-zA-ZÑ\.]{0,15}\s{0,1}[a-zA-ZÑ\.]{0,15}\s{0,1}[a-zA-ZÑ\.]{0,15})'
str_payee6 = '(dtd\s\d{2}-\d{2}-\d{2}[\s\-]{0,3}[a-zA-ZÑ\.,]{1,15}\s{0,1}[a-zA-ZÑ\.]{0,15}\s{0,1}[a-zA-ZÑ\.]{0,15}\s{0,1}[a-zA-ZÑ\.]{0,15})'
df_engas_noPayee5['PAYEE'] = df_engas_noPayee5['PARTICULARS2'].str.extract(str_payee6)
df_engas_noPayee5['PAYEE'] = df_engas_noPayee5['PAYEE'].str.replace(r'(dtd\s\d{2}-\d{2}-\d{2}[\s\-]{0,3})','')
df_engas_noPayee5['PAYEE'] = df_engas_noPayee5['PAYEE'].str.upper()


# In[24]:


#separate6 records with Payee
df_engas_noPayee6 = df_engas_noPayee5[df_engas_noPayee5['PAYEE'].isnull()].copy()
df_engas_Payee6 = df_engas_noPayee5[df_engas_noPayee5['PAYEE'].notnull()].copy()

# Extract7
str_payee7 = '(\d{5,10}\s{0,2}[A-Z\-]{1,3}\s{0,2}\s{0,2}[A-ZÑ\.,]{2,15}\s{0,2}[A-ZÑ\.]{0,15}\s{0,1}[A-Z\.]{0,15}\s{0,1}[A-ZÑ\.]{0,15}\s{0,1}[A-ZÑ]{0,15}\s{0,1}[A-Z]{0,15})'
df_engas_noPayee6['PAYEE'] = df_engas_noPayee6['PARTICULARS2'].str.extract(str_payee7)
df_engas_noPayee6['PAYEE'] = df_engas_noPayee6['PAYEE'].str.replace(r'(\d{5,10}\s{0,2}[A-Z\-]{1,3}\s{0,2}\s{0,2})','')
df_engas_noPayee6['PAYEE'] = df_engas_noPayee6['PAYEE'].str.replace(r'(\s[PI]{1}$)','')
df_engas_noPayee6['PAYEE'] = df_engas_noPayee6['PAYEE'].str.replace(r'(PAYMENT[A-Z\s]{1,50})','')
df_engas_noPayee6['PAYEE'] = df_engas_noPayee6['PAYEE'].str.replace(r'(\sET. AL)','')


# In[25]:


#separate7 records with Payee
df_engas_noPayee7 = df_engas_noPayee6[df_engas_noPayee6['PAYEE'].isnull()].copy()
df_engas_Payee7 = df_engas_noPayee6[df_engas_noPayee6['PAYEE'].notnull()].copy()

# Extract8
str_payee8 = '([\,\-]{1}\s*201[5]*[78]{1}5*\,*\s\-*\s*[A-Z3]{1}[a-zA-ZÑ3\.]{0,15}\s{0,1}[a-zA-Z3\.]{0,15}\s{0,1}[a-zA-ZÑ\.]{0,15}\s{0,1}[a-zA-ZÑ]{0,15}\s{0,1}[a-zA-Z]{0,15})'
df_engas_noPayee7['PAYEE'] = df_engas_noPayee7['PARTICULARS'].str.extract(str_payee8)
df_engas_noPayee7['PAYEE'] = df_engas_noPayee7['PAYEE'].str.replace(r'([\,\-]{1}\s*201[5]*[78]{1}5*\,*\s\-*\s*)','')
df_engas_noPayee7['PAYEE'] = df_engas_noPayee7['PAYEE'].str.upper()
df_engas_noPayee7['PAYEE'] = df_engas_noPayee7['PAYEE'].str.replace(r'(\sET[.\sAL]{0,4})','')
df_engas_noPayee7['PAYEE'] = df_engas_noPayee7['PAYEE'].str.replace(r'(SOA NO.)','')


# In[26]:


#separate8 records with Payee
df_engas_noPayee8 = df_engas_noPayee7[df_engas_noPayee7['PAYEE'].isnull()].copy()
df_engas_Payee8 = df_engas_noPayee7[df_engas_noPayee7['PAYEE'].notnull()].copy()

# Extract9
str_payee9 = '([A-ZÑ3\.]{0,15}\s{0,1}[A-ZÑ3\.\,]{0,15}\s{0,1}[A-Z3\.\,]{0,15}\s{0,1}[A-ZÑ\.]{0,15}\s*\,*\s{0,1}ET.\s*AL)'
df_engas_noPayee8['PAYEE'] = df_engas_noPayee8['PARTICULARS2'].str.upper()
df_engas_noPayee8['PAYEE'] = df_engas_noPayee8['PAYEE'].str.extract(str_payee9)
df_engas_noPayee8['PAYEE'] = df_engas_noPayee8['PAYEE'].str.replace(r'(,*\s{0,1}ET.\s*AL)','')
df_engas_noPayee8['PAYEE'] = df_engas_noPayee8['PAYEE'].str.replace(r'(^\s)','')  


# In[27]:


#separate9 records with Payee
df_engas_noPayee9 = df_engas_noPayee8[df_engas_noPayee8['PAYEE'].isnull()].copy()
df_engas_Payee9 = df_engas_noPayee8[df_engas_noPayee8['PAYEE'].notnull()].copy()
    
# Extract10
str_payee10 = '([A-ZÑ\.]{3,15}\s*[A-ZÑ\.\,]{2,15}\s*[A-Z\.\,]{2,15}\s*[A-ZÑ\.]{3,15}\s*[A-ZÑ\.]{0,15})'
df_engas_noPayee9['PAYEE'] = df_engas_noPayee9['PARTICULARS2'].str.extract(str_payee10)
df_engas_noPayee9['PAYEE'] = df_engas_noPayee9['PAYEE'].str.replace(r'(\s[A-Z]{1}$)','') 
df_engas_noPayee9['PAYEE'] = df_engas_noPayee9['PAYEE'].str.replace(r'(DEBIT ADVICE [CT.]{0,3} NO.)','') 


# In[28]:


#separate10 records with Payee
df_engas_noPayee10 = df_engas_noPayee9[df_engas_noPayee9['PAYEE'].isnull()].copy()
df_engas_Payee10 = df_engas_noPayee9[df_engas_noPayee9['PAYEE'].notnull()].copy()


str_payee11 = '(\-\s*[A-Z][a-zñ\,]{4,15}\s[A-Z][a-z]{3,15}\s[A-Z][a-z]{3,15}\s*[A-Z]{0,1}[a-z]{0,15})'
df_engas_noPayee10['PAYEE'] = df_engas_noPayee10['PARTICULARS2'].str.extract(str_payee11)
df_engas_noPayee10['PAYEE'] = df_engas_noPayee10['PAYEE'].str.replace(r'(\-\s*)','') 

df_engas_noPayee10['PAYEE'] = df_engas_noPayee10['PAYEE'].str.upper()

#df_engas_noPayee10['PAYEE'] = df_engas_noPayee10['PAYEE'].str.extract(r'(Aiza L. Dilidili)')


# In[29]:


# Join dataframe
df_engas_payee  = pd.concat([df_engas_Payee,df_engas_Payee2,df_engas_Payee3,df_engas_Payee4,
 df_engas_Payee5,df_engas_Payee6,df_engas_Payee7,df_engas_Payee8,df_engas_Payee9,df_engas_Payee10,df_engas_noPayee10]) 


# In[30]:


df_engas_payee .drop(['PARTICULARS2'],  axis = 1, inplace = True)


# In[31]:


len(df_engas_payee)


# In[32]:


df_engas_payee .drop_duplicates(keep = "first", inplace = True)


# In[33]:


df_engas = df_engas_payee.copy()
len(df_engas) #20618


# #### Export to Excel 

# In[34]:


#export_excel = df_engas_payee .to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_payee.xlsx', index = None, header=True)


# ### EXTRACT CHECK FROM PARTICULARS

# In[35]:


df_engas['PARTICULARS2'] = df_engas['PARTICULARS'].str[:50]
df_engas['PARTICULARS2'] = df_engas['PARTICULARS2'].str.upper()


# In[36]:


#remove rows without check no : Particulars not starting with a check_no
df_engas['REMARKS'] =  df_engas['PARTICULARS2'].str.extract(r'(^[A-Z]{3,10})')

noCheck = df_engas[df_engas['REMARKS'].notnull()].copy()   #8248
toCheck1 = df_engas[df_engas['REMARKS'].isnull()].copy()   #12370


# In[37]:


len(noCheck)


# In[38]:


# remove rows with 3 or more string in Particulars2 
toCheck1['PARTICULARS2'] = toCheck1['PARTICULARS2'].str[:20]
toCheck1['REMARKS'] = toCheck1['PARTICULARS2'].str.extract(r'([A-Z]{3,5})')


# In[39]:


#### separte REMARKS with "AND" from the datafame; some of them are valid 
toCheck2a = toCheck1.loc[toCheck1['REMARKS'] == 'AND'].copy()
toCheck2b = toCheck1[toCheck1['REMARKS'].isnull()].copy()  

toCheck2 = pd.concat([toCheck2a, toCheck2b], sort = False)
toCheck2.drop(['PARTICULARS2','REMARKS'], axis = 1, inplace= True) 

oneCk = toCheck1[toCheck1['REMARKS'].notnull()].copy()  
oneCk = oneCk[oneCk['REMARKS'] != 'AND']


# In[40]:


# Concatenate df with no check and one check only
df_engas1 = pd.concat([noCheck, oneCk], sort = False)
df_engas1.drop(['PARTICULARS2','REMARKS'], axis = 1, inplace= True) 
# len(df_engas1)  18797  # for *FINAL1 


# In[41]:


df_engas1.head(2)


# In[42]:


# export_excel = df_engas1.to_excel(r'D:\Python\engas_cashdisb\Sept2020ched_engas1.xlsx', index = None, header=True)


# In[43]:


#toCheck2 219
# filter check from ADA - single check only 

toCheck2['ADA_NO'] = toCheck2['ADA_NO'].str.upper()
toCheck2['CHECK_NO2'] = toCheck2['ADA_NO'].str.extract(r'(^\d{5,8}[\sA-Z\;\,]{0,10}$)')
toCheck2['CHECK_NO2'] = toCheck2['CHECK_NO2'].str.replace(r'([\sA-Z\;\,]{0,10})', '')


# In[44]:


df_engas2 = toCheck2[toCheck2['CHECK_NO2'].notnull()].copy()   # 75 rows  for *FINAL2 with check_no extracted from ADA_NO
toCheck3 = toCheck2[toCheck2['CHECK_NO2'].isnull()].copy()     # 116 rows  ADA_NO field is NaN to get from Particulars


# In[45]:


len(toCheck3)


# In[46]:


df_engas2['CHECK_NO2'] = df_engas2['CHECK_NO2'].str.strip() 
df_engas2['CHECK_NO'] = df_engas2['CHECK_NO2'].str.replace(r'(^99[0]{2,3})','990000')
df_engas2.drop(['CHECK_NO2'], axis=1, inplace=True)
df_engas2.head(1)


# In[47]:


toCheck3.head(1)


# In[48]:


# toCheck3 116 rows
# extract single Check_no from Particulars 
toCheck3['PARTICULARS2'] = toCheck3['PARTICULARS'].str[:21]
toCheck3['PARTICULARS2'] = toCheck3['PARTICULARS2'].str.upper()
toCheck3['CHECK_NO2'] = toCheck3['PARTICULARS2'].str.extract(r'(^\d{5,10}[A-ZÑ\s\-\.\;\,]{12})')
toCheck3['CHECK_NO2'] = toCheck3['CHECK_NO2'].str.replace(r'([A-ZÑ\s\-\.\;\,]{12})','')
toCheck3.drop(['PARTICULARS2'], axis = 1, inplace = True)


# In[49]:


toCheck3.head()


# In[50]:


Check3_ok = toCheck3[toCheck3['CHECK_NO2'].notnull()].copy()  # 12 rows  For *FINAL2 with single CHECK_NO2 from Particulars
toCheck4 = toCheck3[toCheck3['CHECK_NO2'].isnull()].copy()  # 104 rows entries for Final3


# In[51]:


len(toCheck4)


# In[52]:


# toCheck4 126 rows
#part 1 extract those rows without ADA Reference
toCheck4_wADA = toCheck4[toCheck4['ADA_NO'].notnull()].copy()  #80 rows out of 126 entries for Final3
toCheck4_noADA = toCheck4[toCheck4['ADA_NO'].isnull()].copy()  #24 rows out of 126 entries for Final3


# In[53]:


toCheck4_noADA.head()


# In[54]:


toCheck4_noADA['PARTICULARS2'] = toCheck4_noADA['PARTICULARS'].str[:55]
toCheck4_noADA['PARTICULARS2'] = toCheck4_noADA['PARTICULARS2'].str.upper() 
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['PARTICULARS2'].str.replace('AND','&') 
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'(D[ATED]{2,4})','XXX')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'(XXX[A-Z\d\;\,\.\s\S]{10,35})','')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'(\d{1,2}\,\s2017)','')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'([A-ZÑ&\s\-\,\.]{3,50}$)','')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace('TO','to') 
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'([A-Z] to [A-Z])','') 
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'([A-ZÑ])','')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'(\-\s{1,2}&)',',')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'(\/)','')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'([&\;]{1,2})',',')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace('- to','-')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace('to,','-')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace('to','-')
toCheck4_noADA['ADA_NO2'] = toCheck4_noADA['ADA_NO2'].str.replace(r'(\-\s*\,)',',')


# In[55]:


toCheck4_noADA.head()


# In[56]:


toCheck4_wADA2 = toCheck4_noADA.copy() 
toCheck4_wADA2.drop(['PARTICULARS2'], axis=1, inplace = True)


# In[57]:


#export_excel = toCheck4_wADA2.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_toCheck4_wADA2.xlsx', index = None, header=True)


# In[58]:


# clean up toCheck4_wADA 
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO'].str.replace('TO','to')
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'([A-Z] to [A-Z])','') 
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'([A-ZÑ])','')                                                             
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'(\;$)','')
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'(;\s*&)',',')
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'(\-\s;)',',')
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'- -',',') 
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'--','-') 
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(';',',')   
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'(\-\/)',',')  
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace('>','')   
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace('-to-',', ')
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace('to','-')
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace('-,',',') 
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'(\-$)',',') 
toCheck4_wADA['ADA_NO2'] = toCheck4_wADA['ADA_NO2'].str.replace(r'(\,$)','') 


# In[59]:


toCheck4_wADA.head(1)


# In[60]:


#export_excel = toCheck4_wADA.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_toCheck4_wADA.xlsx', index = None, header=True)


# In[61]:


# Concat toCheck with ADA
toCheck_ADA = pd.concat([toCheck4_wADA, toCheck4_wADA2], sort = False) # for Final3
len(toCheck_ADA) #104 rows


# In[62]:


# exceptions :  manual clean-up
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99005280 , 99005281-5283','99005280-99005283')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99006388 - 99006389-6394','99006388-99006394')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99006565-6559  99006569','99006565-99006569')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99006748 , 99006749 - 6755','99006748-99006755')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99006806 , 6807 - 6809','99006806-99006809')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99006943 , 99006944 - 6946','99006943- 99006946')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99004762 - 99004762-99004762','99004762')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99005617 - 9900535 , 99005636','99005617-99005636')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('9904771  , 99004772 - 99004776','99004771-99004776')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99005543-99005554 , 9900555','99005543-99005555')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99004999 - 99004997','99004996, 99004997')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99005438,39, 40','99005438-99005440')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99006012 99006013','99006012,99006013')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99005159  , 9900516','99005159,99005160')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('9900541  , 99006554','99006551-99006554')
toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99007529-90007533','99007529-99007533')
#toCheck_ADA['ADA_NO2'] = toCheck_ADA['ADA_NO2'].str.replace('99005696-97','99005696-99005697')


# In[63]:


#export_excel = toCheck_ADA.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_toCheck_ADA.xlsx', index = None, header=True)


# In[64]:


toCheck_ADA.head(1) # for FINAL 3


# In[65]:


# get Column TRANSACTION and ADA_NO2
ADA = toCheck_ADA.copy()
ADA['ADA'] = ADA['ADA_NO2']
del_column = ['JEV_NO','PARTICULARS','ENTRY_DATE','AMOUNT','CHECK_NO','ADA_NO','REFERENCE','SUBSIDIARY_ACCOUNT','MAJOR_ACCOUNT', 'TRANSACTION_TYPE','YEAR_ENTRY','ACCOUNT_FLAG','PAYEE','CHECK_NO2','ADA_NO2']
ADA.drop(del_column, axis = 1, inplace = True)


# In[66]:


ADA


# In[67]:


#export_excel = toCheck_ADA.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_toCheck_ADA.xlsx', index = None, header=True)


# In[68]:


# split data with (,)
def Split_Comma(df) :
    new_df = df['ADA'].str.split(",").apply(pd.Series, 1).stack()
    new_df.index = new_df.index.droplevel(-1)
    df.drop(['ADA'], axis=1, inplace=True)
    new_df.name = 'ADA'
    split_df = pd.merge(df, new_df, left_index=True, right_index=True)
    return split_df


# In[ ]:


ADA = Split_Comma(ADA)
ADA['ADA'] = ADA['ADA'].str.strip() 
ADA['ADA'] = ADA['ADA'].apply(lambda x : '9900' + x if len(x) <= 4 else x)


# In[ ]:


ADA.head()


# In[ ]:


#export_excel = ADA.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_split_ADA.xlsx', index = None, header=True)


# In[ ]:


# Extract ADA with check series or with (-)  
ADA['ADA2'] = ADA['ADA'].str.extract(r'(\d{4,8}\s*\-\s*\d{4,8})')
New_ADA1 = ADA[ADA['ADA2'].isnull()].copy()  # 96 rows - ok
New_ADA2 = ADA[ADA['ADA2'].notnull()].copy()  # 87 rows  to split (-)

New_ADA1.drop(['ADA2'], axis = 1, inplace = True)
New_ADA2.drop(['ADA2'], axis = 1, inplace = True)


# In[ ]:


New_ADA1.head() #This is ok for CHECK_NO2 (to join with ____)


# In[ ]:


New_ADA2.head()


# In[ ]:


# get number series
def getSeries(a, b):
    c = b-a-1
    ada = []
    if c == -1 :
        ada.append(a)  
    elif c == 0 :
        ada.append(a)
        ada.append(b)
    else : 
        ctr = 0
        while ctr <= c :
            ada.append(a)
            a += 1
            ctr += 1
        ada.append(b)
    return ada


# In[ ]:


# split number series with (-)
def Split_Series(df) :
    df[['ADA1','ADA2']] = df['ADA'].str.split("-", expand=True)
    #df.drop(['ADA'], axis =1, inplace = True)
    df['ADA2'] = df['ADA2'].str.strip()
    df['ADA2'] = df['ADA2'].apply(lambda x : '9900' + x if len(x) <= 4 else x)
    df[['ADA1','ADA2']]  = df[['ADA1','ADA2']].apply(pd.to_numeric)
    return df


# In[ ]:


New_ADA2x = Split_Series(New_ADA2)
New_ADA2x['ADA'] = New_ADA2x.apply(lambda x : getSeries(x.ADA1, x.ADA2), axis=1)
New_ADA2x.drop(['ADA1','ADA2'], axis =1, inplace = True)
New_ADA2x['ADA'] = New_ADA2x['ADA'].astype(str)
New_ADA2x['ADA'] = New_ADA2x['ADA'].str.replace('[','')
New_ADA2x['ADA'] = New_ADA2x['ADA'].str.replace(']','')
New_ADA2 = Split_Comma(New_ADA2x)


# In[ ]:


# Concatenate 
New_ADA = pd.concat([New_ADA1,New_ADA2],  sort = False) # 461 rows


# In[ ]:


New_ADA


# In[ ]:


New_ADA.rename({'TRANSACTION_NO':'TRANSACTION_NO2'}, axis='columns', inplace=True)


# In[ ]:


New_ADA


# In[ ]:


df_engas3 = pd.merge(toCheck_ADA, New_ADA, left_index=True, right_index=True)
df_engas3['CHECK_NO'] = df_engas3['ADA'].str.strip() 
df_engas3['CHECK_NO'] = df_engas3['CHECK_NO'].str.replace(r'(^99[0]{2,4})','990000')
df_engas3['ADA_NO'] = df_engas3['ADA_NO2']
df_engas3.drop(['CHECK_NO2','ADA_NO2','TRANSACTION_NO2','ADA'], axis = 1, inplace= True) 
#len(df_engas2)


# In[ ]:


df_engas3.head()


# In[ ]:


#export_excel = df_engas3.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas3.xlsx', index = None, header=True)


# In[ ]:


df_engas_final = pd.concat([df_engas1, df_engas2, df_engas3], sort=False)
df_engas_final.drop_duplicates(keep = 'first', inplace=True)
len(df_engas_final)


# In[ ]:


df_engas_final


# In[ ]:


#export_excel = df_engas_final.to_excel(r'D:\mikael\pandas_output\CHED_CLEANUP\ched_engas_final.xlsx', index = None, header=True)


# In[ ]:


#save dataframe to table
# params = urllib.parse.quote_plus('Driver={SQL Server};SERVER=' + server + ';DATABASE=' + db2 + '; Trusted_Connection=yes')
# engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
df_engas_final.to_sql("ENGAS_DISB_CHED", engine, if_exists='replace' )


# In[ ]:




