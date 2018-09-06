import pandas as pd
import numpy as np
# from sklearn.preprocessing import Imputer

df_origin = pd.read_csv('dwd_rent_cp_rate_0724.csv', sep='\t')
# view = df_origin.describe(percentiles=[0.1,0.25,0.5,0.75,0.9]).astype(np.int64).T

columns_fill_zero = ['num_withdraw','order_num','company_id']
df_train_fill_zero = df_origin[columns_fill_zero]
df_train_zero = df_train_fill_zero[ ~df_train_fill_zero['order_num'].isnull() ]
df_train_not_zero = df_train_fill_zero[ df_train_fill_zero['order_num'].isnull() ]
df_train_zero = df_train_zero[['num_withdraw','company_id']]
df_train_not_zero = df_train_not_zero[['num_withdraw','company_id']]
df_train_zero = df_train_zero.fillna(0)
low_series_zero = df_train_fill_zero.max(axis=0)
df_train_not_zero = df_train_not_zero.fillna(value=low_series_zero)
df_train_fill_zero = pd.concat([df_train_zero,df_train_not_zero],axis=0)
df = df_origin.drop(['num_withdraw'],axis=1)
df = df.merge(df_train_fill_zero, how='left',on='company_id')

columns_high = ['order_num','gmv_cp','valid_offer_ratio','order_city_ratio','num_car_rent','offer_num','remit_ratio']
columns_low = ['avg_contact_period','avg_fetch_period']
df_train_high = df[columns_high]
df_train_low = df[columns_low]
df_train_fill_zero = df['num_withdraw']

high_series = df_train_high.min(axis=0)
low_series = df_train_low.max(axis=0)
df_train_high = df_train_high.fillna(value = high_series)
df_train_low = df_train_low.fillna(value = low_series)
# df_train_fill_zero = df_train_fill_zero.fillna(0)


df_update = pd.concat([df_train_high,df_train_low,df_train_fill_zero],axis=1)

view = df_update.describe(percentiles=[0.1,0.25,0.5,0.75,0.9]).astype(np.int64).T
rate_whole = pd.Series(data=[17.5,15,12.5,10,7.5,5,2.5,13,7,10],index=['order_num','gmv_cp','valid_offer_ratio','order_city_ratio','num_withdraw','num_car_rent','offer_num','avg_contact_period','avg_fetch_period','remit_ratio'])

def get_rate(data,attr):
    temp = data[attr]
    if attr not in ['num_withdraw','avg_contact_period','avg_fetch_period']:
        if temp >= view.loc[attr,'90%']:
            return rate_whole[attr]
        elif temp >= view.loc[attr,'75%']:
            return rate_whole[attr]*0.75
        elif temp >= view.loc[attr,'50%']:
            return rate_whole[attr]*0.5
        elif temp >= view.loc[attr,'25%']:
            return rate_whole[attr]*0.25
        else:
            return 0
    else:
        if temp <= view.loc[attr,'10%']:
            return rate_whole[attr]
        elif temp <= view.loc[attr,'25%']:
            return rate_whole[attr]*0.75
        elif temp <= view.loc[attr,'50%']:
            return rate_whole[attr]*0.5
        elif temp <= view.loc[attr,'75%']:
            return rate_whole[attr]*0.25
        else:
            return 0

attr_columns=['order_num','gmv_cp','valid_offer_ratio','order_city_ratio','num_withdraw','num_car_rent','offer_num','avg_contact_period','avg_fetch_period','remit_ratio']
rates = []
for x in df_update.index:
    data = df_update.loc[x]
    rate = 0
    for attr in attr_columns:
        rate += get_rate(data,attr)
    rates.append(rate)
df_update['rate'] = rates
df_origin['rate'] = rates

df_city = pd.read_csv('company_info.csv',sep='\t')
df_origin = df_origin.merge(df_city,how='left',on='company_id')

df_origin.to_excel('cp_rating.xlsx')
df_origin.to_csv('cp_rating.csv',sep='\t',header = False)
# view.to_excel('cp_attr_describe.xlsx')
view_rate = df_origin['rate'].describe(percentiles=[0.25,0.5,0.75,0.9])
