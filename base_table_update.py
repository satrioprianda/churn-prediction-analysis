# For wrangling and numerical process
import pandas as pd
import numpy as np

# For scheduling and stuff
from datetime import timedelta, datetime
from matplotlib import pyplot as plt

# For SQL importing
from sqlalchemy import create_engine, types
from datetime import datetime


# Basic needs
import os
import requests
import shutil
import time

pd.options.display.max_rows = 500
pd.options.display.max_columns = 100

# 
import seaborn as sns


import warnings
warnings.filterwarnings('ignore')
from datetime import datetime

def get_the_boundary_date(input_date):
    today = datetime.strptime(input_date, '%Y%m%d')
    yesterday= today- timedelta(days=30)

    # yesterday = today - datetime.timedelta(days=30)

    # print(yesterday)
    # yesterday.month
    year= yesterday.year
    month = yesterday.month
    date= yesterday.day
    lower_date= str(year)+'-0'+ str(month)+'-01'
    upper_date= str(today.year)+'-0'+ str(today.month)+'-01'
    return list((lower_date,upper_date))
def  get_dataframe(inputdata):
    if inputdata == 'sales':
        output= pd.read_sql(query_2, engine)
    elif inputdata =='maxel' :
        output=  pd.read_sql(query_1, engine)
    elif inputdata == 'mean':
        output=  pd.read_sql(query3, engine)
    else:
        output= 'empty'
    return output
def get_maxel_data(sales_data,ap_weighting):
    join_sales= pd.merge(sales_data,ap_weighting, left_on= 'sales_date',right_on='day_date',how='left')
    # join_sales_full['total_credit']= join_sales_full['total_credit']/1000000000
    join_sales= join_sales.sort_values('sales_date')
    join_sales= join_sales.reset_index(drop=True)
    join_sales['weighting']= join_sales['weighting']*100
    join_sales['date']= join_sales['sales_date'].dt.day
    join_sales['amt_credit']=join_sales['amt_credit']/1000000000
    join_sales['total_credit']= join_sales['total_credit']/1000000000


    listcode_year= list(join_sales['code_year_month'].unique())
    aa= []
    indexnya= []
    for a in listcode_year[0:41]:


        tt= join_sales[join_sales['code_year_month']== a]
        index= join_sales[join_sales['code_year_month']== a].index.to_list()
        tt['cumsum_weight']=tt['weighting'].cumsum()
        tt['cumsum_sales']=(tt['amt_credit'].cumsum())
        final= (100/tt['cumsum_weight'])*tt['cumsum_sales']
        aa.extend(final)
        indexnya.extend(index)

    kosong = pd.DataFrame()
    kosong['index']= indexnya
    kosong['pred_month_sales']= aa
    # kosong
    join_sales_full= pd.concat([join_sales,kosong['pred_month_sales']],axis=1)


#
    join_sales_full['prediction_residual']=(join_sales_full['pred_month_sales']- join_sales_full['total_credit'])
    join_sales_full['prediction_residual_perc']= ((join_sales_full['pred_month_sales']- join_sales_full['total_credit'])/join_sales_full['total_credit'])*100
    join_sales_full['amt_credit']=join_sales_full['amt_credit']
    join_sales_full['daily_sales_predicted']=(join_sales_full['weighting']* join_sales_full['pred_month_sales'])/100
    join_sales_full['daily_sales_predicted_residual']=(join_sales_full['daily_sales_predicted']- join_sales_full['amt_credit'])
    join_sales_full['daily_residual_percentage']=(join_sales_full['daily_sales_predicted_residual']/join_sales_full['amt_credit'])*100
    return join_sales_full



def get_mean_model_data(mean_model, ap_weighting,sales_data):
    mean_model= pd.merge(mean_model,ap_weighting[['day_date','code_year_month']], how= 'left', left_on= 'sales_date',right_on='day_date').drop(columns='day_date')
    
    join_mean_model= pd.merge(sales_data,mean_model, left_on= 'sales_date',right_on='sales_date',how='left')
    # join_sales_full['total_credit']= join_sales_full['total_credit']/1000000000
    join_mean_model= join_mean_model.sort_values('sales_date')
    join_mean_model= join_mean_model.reset_index(drop=True)
    # join_mean_model['weighting']= join_mean_model['percentage_mean_model']*100
    join_mean_model['date']= join_mean_model['sales_date'].dt.day
    join_mean_model['amt_credit']=join_mean_model['amt_credit']/1000000000
    join_mean_model['total_credit']= join_mean_model['total_credit']/1000000000
    join_mean_model['weighting']= join_mean_model['percentage_mean_model']
    join_mean_model= join_mean_model[join_mean_model['code_year_month'].notnull()]
    join_mean_model= join_mean_model.reset_index(drop=True)
    
    
    listcode_year_mean= list(join_mean_model['code_year_month'].unique())
    aa_mean= []
    indexnya_mean= []
    for a in listcode_year_mean[0:len(listcode_year_mean)]:


        tt_mean= join_mean_model[join_mean_model['code_year_month']== a]
        index_mean= join_mean_model[join_mean_model['code_year_month']== a].index.to_list()
        tt_mean['cumsum_weight']=tt_mean['weighting'].cumsum()
        tt_mean['cumsum_sales']=(tt_mean['amt_credit'].cumsum())
        final_mean= (100/tt_mean['cumsum_weight'])*tt_mean['cumsum_sales']
        aa_mean.extend(final_mean)
        indexnya_mean.extend(index_mean)

    kosong_mean = pd.DataFrame()
    kosong_mean['index']= indexnya_mean
    kosong_mean['pred_month_sales']= aa_mean
    # kosong
    join_mean_model_full= pd.concat([join_mean_model,kosong_mean['pred_month_sales']],axis=1)


#
    join_mean_model_full['prediction_residual']=(join_mean_model_full['pred_month_sales']- join_mean_model_full['total_credit'])
    join_mean_model_full['prediction_residual_perc']= ((join_mean_model_full['pred_month_sales']- join_mean_model_full['total_credit'])/join_mean_model_full['total_credit'])*100

    join_mean_model_full['daily_sales_predicted']=(join_mean_model_full['weighting']* join_mean_model_full['pred_month_sales'])/100
    join_mean_model_full['daily_sales_predicted_residual']=(join_mean_model_full['daily_sales_predicted']- join_mean_model_full['amt_credit'])
    join_mean_model_full['daily_residual_percentage']=(join_mean_model_full['daily_sales_predicted_residual']/join_mean_model_full['amt_credit'])*100
    join_mean_model_full=join_mean_model_full.rename(columns = {'weighting':'weighting_mean',
                                          'pred_month_sales':'pred_month_sales_mean',
                                          'prediction_residual':'prediction_residual_mean',
                                           'prediction_residual_perc':'prediction_residual_perc_mean',
                                           'daily_sales_predicted':'daily_sales_predicted_mean',
                                           'daily_sales_predicted_residual':'daily_sales_predicted_residual_mean',
                                           'daily_residual_percentage':'daily_residual_percentage_mean'

                                          })
    return join_mean_model_full


def get_final_table(join_sales_full,join_mean_model_full):
    list_mean= ['sales_date','weighting_mean','pred_month_sales_mean','prediction_residual_mean','prediction_residual_perc_mean',
           'daily_sales_predicted_mean','daily_sales_predicted_residual_mean','daily_residual_percentage_mean']
    merge_maxel_mean= pd.merge(join_sales_full,join_mean_model_full[list_mean], on='sales_date', how='left')

    list_mean= ['sales_date','weighting_mean','pred_month_sales_mean','prediction_residual_mean','prediction_residual_perc_mean',
            'daily_sales_predicted_mean','daily_sales_predicted_residual_mean','daily_residual_percentage_mean']
    merge_maxel_mean= pd.merge(join_sales_full,join_mean_model_full[list_mean], on='sales_date', how='left')

    # 1.
    cols_mape= ['prediction_residual_perc_mean','prediction_residual_perc']
    cols_mape_new= ['absolute_month_mean_perc_res','absolute_month_maxel_perc_res']
    merge_maxel_mean[cols_mape_new]=merge_maxel_mean[cols_mape].astype(float).abs()



    cols_daily_mape= ['daily_residual_percentage_mean','daily_residual_percentage']
    cols_daily_mape_new=['abs_daily_perc_mean','abs_daily_perc_maxel']
    merge_maxel_mean[cols_daily_mape_new]=merge_maxel_mean[cols_daily_mape].abs()




    # 2

    cols_mse_month= ['prediction_residual_mean','prediction_residual']
    cols_mse_month_new= ['mse_month_res_mean','mse_month_res_maxel']
    merge_maxel_mean[cols_mse_month_new]=merge_maxel_mean[['prediction_residual_mean','prediction_residual']]**2

    cols_mse_daily= ['daily_sales_predicted_residual','daily_sales_predicted_residual_mean']
    cols_mse_daily_new= ['mse_daily_res_maxel','mse_daily_res_mean']

    merge_maxel_mean[cols_mse_daily_new]=merge_maxel_mean[cols_mse_daily]**2
    merge_maxel_mean[cols_mse_daily+cols_mse_daily_new].tail(2)




    # 3
    cols_mae_month=['prediction_residual_mean','prediction_residual']
    cols_mae_month_new= ['mae_month_res_mean','mae_month_res_maxel']

    merge_maxel_mean[cols_mae_month_new]=merge_maxel_mean[cols_mae_month].abs()
    # merge_maxel_mean[cols_mae_month+cols_mae_month_new]

    cols_mae_daily = ['daily_sales_predicted_residual','daily_sales_predicted_residual_mean']
    cols_mae_daily_new= ['mae_daily_res_maxel','mae_daily_res_mean']
    merge_maxel_mean[cols_mae_daily_new]=merge_maxel_mean[cols_mae_daily].abs()
    # merge_maxel_mean[cols_mae_daily+cols_mae_daily_new].tail()







    # 4


    cols_rmse_month= ['rmse_month_res_mean','rmse_month_res_maxel']
    merge_maxel_mean[cols_rmse_month]=np.sqrt(merge_maxel_mean[cols_mse_month_new])

    cols_rmse_daily= ['rmse_daily_res_maxel','rmse_daily_res_mean']
    merge_maxel_mean[cols_rmse_daily]=np.sqrt(merge_maxel_mean[cols_mse_daily_new])
    # merge_maxel_mean[cols]





    # RENAME
    merge_maxel_mean= merge_maxel_mean.rename(columns={'daily_sales_predicted_residual_mean':'daily_sales_pred_res_mean',
                                                'daily_sales_predicted_residual':'daily_sales_predicted_res',
                                                'daily_residual_percentage_mean':'daily_residual_perc_mean'})





    return merge_maxel_mean

def get_the_residual(daily,predicted):
    
    if predicted.isnull() is True:
        final_mae= np.nan
        final_mape= np.nan
        final_rmse= np.nan
        final_mse= np.nan
    else:
        final_mae= abs(daily- predicted)
        final_mape= abs(((daily-predicted)/daily)*100)
        final_mse= (daily-predicted)**2
        final_rmse= np.sqrt((daily-predicted)**2)
    return final_mae,final_mape,final_mse,final_rmse

def get_3_day_data(query,merge_maxel_mean):
    
    sales_df= pd.read_sql(query, engine)
    engine.dispose()




    listtomorrow= list((sales_df['tomorrows_pred'].shift(axis=0,periods=1)))
    list_2day= list((sales_df['next_two_days_amt'].shift(axis=0,periods=2)))
    list_today= list((sales_df['todays_pred']))



    final= pd.DataFrame(columns={'sales_date'})
    final['sales_date']=sales_df.date
    final['today'] =list_today
    final['tomorrow']= listtomorrow
    final['next_2day']= list_2day



    # query_sales= """select * from SP_SALES_RR_MON"""
    monitoring= merge_maxel_mean
    engine.dispose()


    columns_day= ['sales_date','amt_credit','daily_sales_predicted','daily_sales_predicted_mean']
    # monitoring[columns_day]
    final_Table = pd.merge(monitoring,final,how='left',on='sales_date')
    return final_Table

def get_final_result(final_Table):
    list_additional= ['MAE','MAPE','MSE','RMSE']

    today= pd.DataFrame()
    for a in range(0,4):

        today[(list_additional[a])+'_today_predicted']= get_the_residual(daily=final_Table['amt_credit'], predicted=final_Table['today'])[a]
    final_Table= pd.concat([ final_Table,today], axis=1)



    tomorrow= pd.DataFrame()
    for a in range(0,4):

        tomorrow[(list_additional[a])+'_tomorrow_predicted']= get_the_residual(daily=final_Table['amt_credit'], predicted=final_Table['tomorrow'])[a]
    final_Table= pd.concat([ final_Table,tomorrow], axis=1)



    aaa= pd.DataFrame()
    for a in range(0,4):
    #     ttt[f'{a}']= get_the_residual(daily=final_Table['amt_credit'], predicted=final_Table['next_2day'])[a]
        aaa[(list_additional[a])+'_next_2day_predicted']= get_the_residual(daily=final_Table['amt_credit'], predicted=final_Table['next_2day'])[a]
    final_Table= pd.concat([ final_Table,aaa], axis=1)
    return final_Table

def get_date():
    query = f"""
        select max(sales_date) from SP_SALES_RR_MON
        """
    sales_df= pd.read_sql(query, engine) 
#     engine.dispose()
    return sales_df

# print('saving to DWH')
# final_Table.to_sql(name='SP_SALES_RR_MON_delete_',con=engine, if_exists='append',index=False)
# print('suuces')
