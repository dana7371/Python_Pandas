import io
import pandas as pd
import requests as r
import datetime as dt

# target URL, file name
url = 'http://drd.ba.ttu.edu/isqs3358/hw2/'
path = 'C:\\Users\\Dana7\\Desktop\\' #will need to change file path


#files
file_1 = 'hr_data.csv'
file_2 = 'sales_data.csv'
title_aggregate = 'title_aggregate.csv'
employee_raise = 'employee_raise.csv'
employee_salary_raises = 'employee_salary_raises.csv'


res = r.get(url + file_1)
res.status_code
df_emp = pd.read_csv(io.StringIO(res.text), delimiter = '|')


res = r.get(url + file_2)
df = pd.read_csv(io.StringIO(res.text), delimiter='|')

df_emp.head()
df.head()

#merging
df_emp_merge = df_emp.merge(df, how="inner", on = "EmpId")
df_emp_merge.head()
print(df_emp_merge)



df_emp_merge['Per_Item_Sale'] = df_emp_merge['SalesValue'] / df_emp_merge['ItemsSold']
df_emp_merge['Total_Compensation'] = df_emp_merge['Salary'] + df_emp_merge['Benefits']
df_emp_merge['Employee_perf_metric'] = (((df_emp_merge['Per_Item_Sale'] / df_emp_merge['Total_Compensation']) * 100) + 67) / df_emp_merge['ItemsSold']

df_emp_merge['Employee_raise_elligible'] = 'No'
df_emp_merge['Employee_raise_elligible'][(df_emp_merge['Title']=='Sales Associate 1') & (df_emp_merge['Employee_perf_metric']>1)] = 'Yes'
df_emp_merge['Employee_raise_elligible'][(df_emp_merge['Title']=='Sales Associate 2') & (df_emp_merge['Employee_perf_metric']>1.2)] = 'Yes'
df_emp_merge['Employee_raise_elligible'][(df_emp_merge['Title']=='Sales Associate 3') & (df_emp_merge['Employee_perf_metric']>3)] = 'Yes'
df_emp_merge['Employee_raise_elligible'][(df_emp_merge['Title']=='Sales Manager') & (df_emp_merge['Employee_perf_metric']>11)] = 'Yes'
print(df_emp_merge)

#if employee_raise is yes then to csv
df_emp_merge[['EmpId','Title','Salary']][(df_emp_merge['Employee_raise_elligible']=="Yes")].head().to_csv(path + employee_raise, sep='|', index=False)

#compute employee salary raise
df_emp_merge['updated_salary'] ='New'
df_emp_merge['updated_salary'][(df_emp_merge['Title']=='Sales Associate 1')] = (df_emp_merge['Salary'] *0.06) + df_emp_merge['Salary']
df_emp_merge['updated_salary'][(df_emp_merge['Title']=='Sales Associate 2')] = (df_emp_merge['Salary'] *0.045) + df_emp_merge['Salary']
df_emp_merge['updated_salary'][(df_emp_merge['Title']=='Sales Associate 3')] = (df_emp_merge['Salary'] *0.04) + df_emp_merge['Salary']
df_emp_merge['updated_salary'][(df_emp_merge['Title']=='Sales Manager')] = (df_emp_merge['Salary'] *0.02) + df_emp_merge['Salary']

df_emp_merge['salary_diff'] = df_emp_merge['updated_salary'] - df_emp_merge['Salary']

#check print
print(df_emp_merge)

sum_df = df_emp_merge[['Salary', 'updated_salary', 'salary_diff','Title']].groupby('Title').sum()
df_emp_merge[['Title','Salary','updated_salary','salary_diff']].head().to_csv(path + employee_salary_raises, sep='|', index = False)


# Constants
TC = 'Total_Compensation'
SV = 'Per_Item_Sale'
EP = 'Employee_perf_metric'

# average total_compensation, Per_Item_Sale, Sale_Employee_perf_metric by Title
IS_and_SV_average_df = df_emp_merge[[TC, SV, EP,'Title']].groupby('Title').mean()
IS_and_SV_average_df.rename(columns={TC: 'avg_' + TC, SV: 'avg_' + SV, EP: 'avg_' + EP}, inplace=True)
df_emp_merge = df_emp_merge.merge(IS_and_SV_average_df, left_on='Title', right_index=True)
df_emp_merge.sort_index(inplace=True)
print(df_emp_merge)

#average to csv file
df_emp_merge[['Title',
    'avg_Total_Compensation',
    'avg_Employee_perf_metric']].head().to_csv(path + title_aggregate, sep='|', index=False)













