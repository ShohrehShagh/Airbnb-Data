import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pandas import DataFrame
###
df= pd.read_csv('Toronto/listings.csv', index_col=None, header=0,parse_dates=True)
data_listing=df[['id','neighbourhood','price','room_type','bedrooms','review_scores_value']]
data_listing.is_copy = False
data_listing=data_listing[(data_listing['room_type']=='Entire home/apt') & (data_listing['bedrooms']==1) &(data_listing['review_scores_value']>6)]
data_listing.rename(columns={'id': 'listing_id'}, inplace=True)
data_listing['price']=data_listing['price'].str.replace('$', '')
data_listing['price']=data_listing['price'].convert_objects(convert_numeric=True)
#
grouped_listing_data=data_listing.groupby(['neighbourhood'])
grouped_listing_data=grouped_listing_data['price'].agg({'Average Price': ['mean']})
ax = grouped_listing_data.plot(kind='bar', title ="Average Price of one-bedroom private apartments with average review score of larger than 7/10  in different neighbourhoods of Toronto",figsize=(40,10),legend=False,fontsize=12, width=1.0)
ax.set_ylabel("Price",fontsize=18)
pos1 = ax.get_position() # get the original position
pos2 = [pos1.x0 - 0.08, pos1.y0 +0.1,  pos1.width*1.22, pos1.height*0.9]
ax.set_position(pos2) # set a new position
#
data_calender= pd.read_csv('Toronto/calendar.csv', index_col=None, header=0,parse_dates=True)
data_calender['date'] = pd.to_datetime(data_calender['date'])
data_calender['month']=data_calender['date'].dt.month
data_calender['year']=data_calender['date'].dt.year
data_calender['price']=data_calender['price'].str.replace('$', '')
data_calender['price']=data_calender['price'].convert_objects(convert_numeric=True)
#
data_merged=pd.merge(data_listing, data_calender, on='listing_id', how='outer')
data_merged=data_merged[['listing_id','neighbourhood','price_x','price_y','month','year']]
data_merged_downtown=data_merged[(data_merged['neighbourhood']=='Downtown Toronto')]

grouped_data_merged_downtown=data_merged_downtown.groupby(['month'])
grouped_data_merged_downtown=grouped_data_merged_downtown['price_y'].agg({'Average Price': ['mean']})


ax=data_merged_downtown.boxplot(column='price_y', by='month', whis=6,figsize=(40,10))
ax.set_title("Average price of one-bedroom private apartments with average review score of larger than 7/10 in downtowns Toronto")
for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +ax.get_xticklabels() + ax.get_yticklabels()):
    item.set_fontsize(18)
#
plt.show()
#
