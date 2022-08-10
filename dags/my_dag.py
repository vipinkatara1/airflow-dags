# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime
# from random import randint


# #necessary libraries
# from pymongo import MongoClient
# import pandas as pd
# import datetime
# import re
# import matplotlib.pyplot as plt
# from collections import Counter
# import numpy as np
# from pprint import pprint
# from sklearn.metrics import mean_squared_error
# from sklearn.metrics import mean_absolute_error
# from sklearn.model_selection import train_test_split
# from datetime import datetime
# import sklearn.metrics
# # import autosklearn.regression
# from xgboost import XGBRegressor


# # In[2]:


# def getClient():
#     try:
#         client = MongoClient()
#     except:
#         print("couldnt connect client")
#         exit()
#     return client


# # In[3]:


# def getDB(client,comName):
#     try:
#         db = client[comName]
#     except:
#         print("invalid company name")
#         exit()
#     return db


# # In[4]:


# def getCollection(db,collectionName):
#     try:
#         collection = db[collectionName]
#     except:
#         print("invalid company name")
#         exit()
#     return collection


# # In[5]:


# def getDate(sDay,eDay,sMonth,eMonth,sYear,eYear):
#     try:
#         start = datetime.datetime(sYear, sMonth, sDay, 0, 0, 0, 0)
#         end = datetime.datetime(eYear, eMonth, eDay, 23, 59, 59, 381)
#     except:
#         print("enter correct time")
#         exit()
#     return start,end


# # In[6]:


# def getData(collection,shopname,start,end):
#     cursor = collection.find({'createdAt': {'$gte': start, '$lt': end}, "shopId": shopname})
#     data = []
#     for i in cursor:
#         data.append(i)
#     return data


# # In[7]:


# def getDF(data):
#     df = pd.DataFrame(data)
#     return df


# # In[8]:


# # def dateTimeformat(df):
# #     df['Date_']=df['createdAt'].dt.strftime('%Y-%m-%d')
# #     df['Days']=df['createdAt'].dt.day_name()
# #     df['Week_no']=df['createdAt'].dt.isocalendar().week
# #     df['hours']=df['createdAt'].dt.hour
# #     df['Months']=df['createdAt'].dt.month_name()
# #     return df


# # In[9]:


# def dateTimeformat(df):
#     df['createdAt'] = df['createdAt'].astype('datetime64[ns]')
#     df['Date_']=df['createdAt'].dt.strftime('%Y-%m-%d')
#     df['Days']=df['createdAt'].dt.day_name()
#     df['Week_no']=df['createdAt'].dt.isocalendar().week
#     df['hours']=df['createdAt'].dt.hour
#     df['Months']=df['createdAt'].dt.month_name()
#     return df


# # In[ ]:





# # In[10]:


# def getNonDenied(df):
#     return df[df['status'] != 'DENIED']


# # In[11]:


# def removeIndex(df):
# #     print(df.index.values)
#     index = list(df.index)
#     delete = []
#     for j in df.index:
#         flag = -1
#         for i in df['orderStatusTimestamps'].loc[j]:
#             if(i['status'] == 'READY_FOR_PICKUP'):
#                 flag = 1
#             else:
#                 pass
#         if(flag == -1):
#             delete.append(j)
#     return delete


# # In[12]:


# def processingStamp(df):
#     arr = []
#     for i in df['orderStatusTimestamps']:
#         flag = 0
#         for j in i:
#             if j['status'] == 'PROCESSING' and flag == 0:
#                 t = j['timestamp']
#                 flag = 1
#         if(flag == 0):
#             t = datetime.datetime(2011, 3, 1, 10, 38, 38, 324000)
#         arr.append(t)
#     return arr


# # In[13]:


# def readyStamp(df):
#     arr = []
#     for i in df['orderStatusTimestamps']:
#         flag = -1
#         for j in i:
#             if j['status'] == 'READY_FOR_PICKUP':
#                 t = j['timestamp']
#                 flag = 1
        
#         if(flag == -1):
#             t = datetime.datetime(1997, 3, 1, 10, 38, 38, 324000)
#         arr.append(t)
#     return arr


# # In[14]:


# from datetime import datetime
# def getStatusDictionary(df):
#     arr= []
#     for i in df['orderStatusTimestamps']:
#         d = {}
#         for j in i:
#             if('PROCESSING' in j.keys() and j['status'] == 'PROCESSING'):
#                 pass
#             else:
#                 d[j['status']] = j['timestamp']
#         arr.append(d)
#     return arr


# # In[15]:


# def timeFormat(df):
#     df['Ready'] = pd.to_datetime(df['Ready'])
#     df['Processing'] = pd.to_datetime(df['Processing'])
#     return df

# def getCookingTime(df):
#     return df['Ready'] - df['Processing']
    
# def timeSeconds(d):
#     return d.total_seconds()/60
# def getTimeSeconds(df):
#     df['time'] = df['time'].apply(timeSeconds)
#     df['time'] = df['time'].apply(int)
#     return df


# # In[ ]:





# # In[ ]:





# # In[16]:


# import datetime
# from datetime import timedelta
# def getQueue(df,TIME,mn):
#     arr = []
#     for i in range(df.shape[0]):
#         if('PROCESSING' not in df['dictionary'].iloc[i]):
#             arr.append(0)
#             continue
#         e = df['dictionary'].iloc[i]['PROCESSING']
#         count = 0
#         for j in range(i-1,-1,-1): 
#             d = df['dictionary'].iloc[j]
#             if 'READY_FOR_PICKUP' in d.keys():
#                 k = 'READY_FOR_PICKUP'
#                 s = d[k]
#             else:
# #                 print(d)
#                 s = e + timedelta(minutes=mn)
#             if((e-s).total_seconds()/60 > TIME):
#                 break
#             if(e < s):
#                 count += 1
#         arr.append(count)
#     df['queue'] = arr
#     return df


# # In[ ]:





# # In[17]:


# def getDishQuantity(df):
#     arr = []
#     for i in range(df.shape[0]):
#         ar = []
#         r = df['kitchenOrderProducts'].iloc[i]
#         for j in r:
#             d = {}
#             if 'refBundleProductId' in j:    
#                 d[j['refBundleProductId']] = j['quantity']
#                 ar.append(d)
#             else:
#                 d[j['refProductId']] = j['quantity']
#                 ar.append(d)
#             if 'selectedBundleProductItems' in j:
            
#                 for k in j['selectedBundleProductItems']:
#                     d = {}
#                     if 'refProductId' in k:
#                         d[k['refProductId']] = j['quantity']
#                         ar.append(d)
#         arr.append(ar)
#     return arr


# # In[ ]:





# # In[18]:


# def k(d):
#     if(len(d) == 0):
#         return ''
#     r = d[0].keys()
#     return list(r)


# # In[19]:


# def k(d):
#     if(len(d) == 0):
#         return ''
#     r = d[0].values()
#     return list(r)


# # In[20]:


# import numpy as np
# def product(a,b):
#     arr = []
#     for i in range(len(a)):
#         for j in range(b[i]):
#             arr.append(a[i])
#     return arr
# def dishQuan(val):
#     arr = []
#     for i in val:
#         d = product(list(i.keys()),list(i.values()))
#         for j in d:
#             arr.append(j)
# #     arr = np.array(arr)
# #     arr = list(arr.ravel())
# #     arr = np.reshape(arr, (np.product(arr.shape),))
#     return list(arr)


# # In[ ]:





# # In[21]:


# import re
# def removeSpace(df):
#     dd = []
#     flag = 0
#     for i in df['finalDish']:
#         for j in range(len(i)):
#             i[j] = i[j].lower()
#             i[j] = i[j].strip()
#             i[j] = i[j].replace("-","")
#             i[j] = i[j].replace("&","and")
#             i[j] = i[j].replace(")","")
#             i[j] = i[j].replace("(","")
#             i[j] = i[j].replace("+","")
#             i[j] = re.sub(' +',' ',i[j])
#             i[j] = i[j].replace(" ","_")
            
#         dd.append(i)
#     df['removedItem'] = dd
#     return df

# def convertString(df):
#     str = []
#     s = " "
#     for i in df['removedItem']:
#         str.append(s.join(i))
#     df['itemString'] = str
#     return df


# # In[22]:


# def deleteThirdParty(df):
#     dele = []
#     for i in df.index:
#         for j in df["removedItem"].loc[i]:
# #             print(j)
#             if 'third_party' in j:
#                 dele.append(i)
#                 break
#     return dele


# # In[23]:


# def getRemoveColumns(df):
#     col = list(df.columns)
#     print(col)
#     c = ['online',
#          'isComment',
#          'isAddon',
#  'driveThrough',
#  'takeAway',
#  'time',
#  'queue',
#  'Days',
#  'Week_no',
#  'hours',
#  'Months','itemString','Date_']
#     for i in c:
#         col.remove(i)
#     return col


# # In[24]:


# from sklearn.feature_extraction.text import CountVectorizer
# def encoding(df, col):
#     df = df.drop(col,axis=1)
#     df['Days'] = df['Days'].map({'Sunday':0, 'Monday':1, 'Tuesday':2, 'Wednesday':3, 'Thursday':4,'Friday':5,"Saturday":6})
#     df['driveThrough'] = df['driveThrough'].map({True:1,False:0})
#     df['online'] = df['online'].map({True:1,False:0})
#     df['takeAway'] = df['takeAway'].map({True:1,False:0})
# #     df['homeDelivery'] = df['homeDelivery'].map({True:1,False:0})
#     df['Months'] = df['Months'].map({'August':7, 'September':8, 'October':9, 'November':10, 'December':11,
#        'January':0, 'February':1, 'March':2, 'April':3, 'May':4, 'June':5, 'July':6})
#     cv = CountVectorizer(ngram_range=(0, 1))
#     x = cv.fit_transform(df['itemString'])
#     df1 = pd.DataFrame(x.todense(),columns=cv.get_feature_names())
#     for c in df.columns:
#         df1[c] = list(df[c])
#     df1 = df1.drop('itemString',axis=1)
#     return df1

# def split(df):
#     y = df['time']
#     x = df.drop('time',axis=1)
#     return x,y
# # df.set_index("Date_", inplace = True)


# # In[ ]:





# # In[25]:


# def getModel(SHOPID,timeLeft=3600,memoryLimit=6000):
#     filename = '/tmp/autosklearn_regression_example_tmpjh1 '+SHOPID
#     automl = autosklearn.regression.AutoSklearnRegressor(time_left_for_this_task=timeLeft,
#         tmp_folder=filename,memory_limit=memoryLimit
#     )

#     return automl


# # In[26]:


# def getMode(df,dele):
#     df1 = df.drop(index=dele)
#     mode = df1['time'].value_counts().index[0]
#     return int(mode)


# # In[27]:


# def getTimeBreak(df,dele):
#     df1=df.drop(index=dele)
#     n = df1.shape[0]
#     timeCounts = df1['time'].value_counts()
#     df2 = pd.DataFrame(timeCounts)
#     df2['percent'] = (df2['time']/n)*100
#     maxTime = max(df2[df2['percent'] >= 1].index)
#     return maxTime
    
    


# # In[28]:


# def getisAddon(df):
#     addOnArray = []
#     for i in df.index:
#         addOn = False
#         for j in df['kitchenOrderProducts'].loc[i]:
#             flag = 0
#             if 'selectedBundleProductItems' in j.keys():
#                 flag = 1
#                 for k in j['selectedBundleProductItems']:
#                     if(len(k['addons']) > 0):
#                         addOn = True
#                         flag = 2
#                         break
#             if(flag == 2):
#                 break
#             if(flag == 0 and len(j['addons']) > 0):
#                 addOn = True
#                 break
#         addOnArray.append(addOn)
#     return addOnArray


# # In[29]:



# def training(x_train,y_train,x_test,y_test):
#     regr = XGBRegressor(base_score=0.5, booster='gbtree', colsample_bylevel=1,
#              colsample_bynode=1, colsample_bytree=0.4, enable_categorical=False,
#              gamma=0.2, gpu_id=-1, importance_type=None,
#              interaction_constraints='', learning_rate=0.01, max_delta_step=0,
#              max_depth=8, min_child_weight=5,
#              monotone_constraints='()', n_estimators=500, n_jobs=8,
#              num_parallel_tree=1, predictor='auto', random_state=0, reg_alpha=0,
#              reg_lambda=1, scale_pos_weight=1, subsample=1, tree_method='exact',
#              validate_parameters=1, verbosity=None)
#     regr.fit(x_train, 
#         y_train,
#         eval_set=[(x_train, y_train), (x_test, y_test)],
#         eval_metric='rmse')
#     return regr



# def mse(regr,x_test,y_test):    
#     testPredict = regr.predict(x_test)
#     mse1 = mean_squared_error(y_test, testPredict)
#     return mse1

# def _data_load():
#     SHOPID = '5c3c4c07febd2d0001c433f4'
#     sDay,sMonth,sYear = 1,3,2021
#     eDay,eMonth,eYear = 22,3,2022
#     client = getClient()
#     db = getDB(client,'qopla')
#     collection = getCollection(db,'kitchenOrder')
#     start,end = getDate(sDay,eDay,sMonth,eMonth,sYear,eYear)
#     data = getData(collection,SHOPID,start,end)
#     df = getDF(data)
#     df = dateTimeformat(df)
#     return df

# def _data_preprocessing(ti):
#     df = ti.xcom_pull(task_id = "data_load")
#     df = getNonDenied(df)
#     df['isComment'] = df['comment'].isnull()
#     df["isAddon"] = getisAddon(df)
#     delete = removeIndex(df)
#     df['Processing'] = processingStamp(df)
#     df['Ready'] = readyStamp(df)
#     df['dictionary'] = getStatusDictionary(df)
#     df = timeFormat(df)
#     df['time'] = getCookingTime(df)
#     df = getTimeSeconds(df)
#     mode = getMode(df,delete)
#     timeBreak = getTimeBreak(df,delete)
#     df = df[df['time'] <= timeBreak]
#     df = getQueue(df,timeBreak,mode)
#     df = df.drop(index=delete)
#     df['dishquantity']= getDishQuantity(df)
#     df['dish']=df['dishquantity'].apply(k)
#     df['quantity']=df['dishquantity'].apply(k)
#     df['finalDish'] = df['dishquantity'].apply(dishQuan)
#     df = removeSpace(df)
#     df = convertString(df)
#     dele = deleteThirdParty(df)
#     df=df.drop(index=dele)
#     col = getRemoveColumns(df)
#     df1 = encoding(df,col)
#     return df1

# def _training_model():
#     return randint(1,10)

# def _choose_best_model(ti):
#     acc = ti.xcom_pull(task_ids = [
#         "training_model_A",
#         "training_model_B",
#         "training_model_C"
#         ])
#     best_accuracy = max(acc)
#     if (best_accuracy > 8):
#         return 'accurate'
#     return "inaccurate"



# with DAG("my_dag", start_date=datetime(2022,7,23),schedule_interval="@daily",catchup=False) as dag:
    
#     data_load = PythonOperator(
#         task_id = "data_load",
#         python_callable=_data_load
#     )
    
#     data_preprocessing = PythonOperator(
#         task_id = "data_preprocessing",
#         python_callable=_data_preprocessing
#     )

#     training_model_A = PythonOperator(
#         task_id = "training_model_A",
#         python_callable=_training_model
#     )
#     training_model_B = PythonOperator(
#         task_id = "training_model_B",
#         python_callable=_training_model
#     )
#     training_model_C = PythonOperator(
#         task_id = "training_model_C",
#         python_callable=_training_model
#     )

#     choose_best_model = BranchPythonOperator(
#         task_id = "choose_best_model",
#         python_callable = _choose_best_model
#     )

#     accurate = BashOperator(
#         task_id = "accurate",
#         bash_command = "echo 'accurate'"
#     )

#     inaccurate = BashOperator(
#         task_id = "inaccurate",
#         bash_command = "echo 'inaccurate'"
#     )

#     data_load >> data_preprocessing >> [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        res = 'accurate'
    else:
        res = 'inaccurate'
    with open("output.log", "w") as f:
        f.write(res)
    return res


def _training_model():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        training_model_B = PythonOperator(
            task_id="training_model_B",
            python_callable=_training_model
        )

        training_model_C = PythonOperator(
            task_id="training_model_C",
            python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate' | tee out.log"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate' | tee out.log"
        )

        [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
