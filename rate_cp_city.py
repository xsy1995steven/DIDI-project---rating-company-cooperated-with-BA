import pandas as pd
import numpy as np
# from sklearn.preprocessing import Imputer

df = pd.read_csv('dwd_rent_cp_rate.csv', sep='\t')



columns_high = ['order_num','gmv_cp','valid_offer_ratio','order_city_ratio','num_car_rent','offer_num','num_remit']
columns_low = ['avg_contact_period','avg_fetch_period']
columns_fill_zero = ['num_withdraw']
# df_train_high = df[columns_high]
# df_train_low = df[columns_low]
# df_train_fill_zero = df[columns_fill_zero]




# high_series = df_train_high.min(axis=0)
# low_series = df_train_low.max(axis=0)
# df_train_high = df_train_high.fillna(value = high_series)
# df_train_low = df_train_low.fillna(value = low_series)
# df_train_fill_zero = df_train_fill_zero.fillna(0)
# df_update = pd.concat([df_train_high,df_train_low,df_train_fill_zero],axis=1)


cities = df['city_id'].drop_duplicates()
df_update = []
for city in cities:
    df_city = df[df['city_id']==city]
    df_train_high = df_city[columns_high]
    df_train_low = df_city[columns_low]
    df_train_fill_zero = df_city[columns_fill_zero]
    high_series = df_train_high.min(axis=0)
    low_series = df_train_low.max(axis=0)
    df_train_high = df_train_high.fillna(value = high_series)
    df_train_low = df_train_low.fillna(value = low_series)
    df_train_fill_zero = df_train_fill_zero.fillna(0)
    df_city_update = pd.concat([df_train_high,df_train_low,df_train_fill_zero],axis=1)
    if len(df_update) == 0:
        df_update = df_city_update
    else:
        df_update = pd.concat([df_update,df_city_update],axis=0)



view = df_update.describe(percentiles=[0.1,0.25,0.5,0.75,0.9]).astype(np.int64).T
rate_whole = pd.Series(data=[17.5,15,12.5,10,7.5,5,2.5,13,7,10],index=['order_num','gmv_cp','valid_offer_ratio','order_city_ratio','num_withdraw','num_car_rent','offer_num','avg_contact_period','avg_fetch_period','num_remit'])

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

attr_columns=['order_num','gmv_cp','valid_offer_ratio','order_city_ratio','num_withdraw','num_car_rent','offer_num','avg_contact_period','avg_fetch_period','num_remit']
rates = []
for x in df_update.index:
    data = df_update.loc[x]
    rate = 0
    for attr in attr_columns:
        rate += get_rate(data,attr)
    rates.append(rate)
df_update['rate'] = rates
df['rate'] = rates


# df.to_excel('cp_rating.xlsx')
# df.to_csv('cp_rating.csv',sep='\t',header = False)
# view.to_excel('cp_attr_describe.xlsx')
view_rate = df['rate'].describe(percentiles=[0.25,0.5,0.75,0.9])
