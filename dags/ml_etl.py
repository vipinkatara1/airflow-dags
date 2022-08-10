import sys
import importlib	
import subprocess
from subprocess import STDOUT, check_call
import os
import subprocess
bashCommand = "apt install -y gcc"
process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
def reqiuiredModule(lib):
    try:
        importlib.import_module(lib)
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install',lib])
reqiuiredModule("sklearn")
reqiuiredModule("bson")
reqiuiredModule("xgboost") # TODO: need to turn it on
reqiuiredModule("sendgrid")
reqiuiredModule("pandas")
reqiuiredModule("numpy")
# necessary libraries
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import re
import datetime
import pandas as pd
#import autosklearn.regression
#from pymongo import MongoClient
# from xgboost import XGBRegressor # TODO: need to turn it on
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from airflow import DAG
from airflow.operators.python import PythonOperator

import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def removeIndex(df):
    #     print(df.index.values)
    index = list(df.index)
    delete = []
    for j in index:
        flag = -1
        for i in df['orderStatusTimestamps'].loc[j]:
            if(i['status'] == 'READY_FOR_PICKUP'):
                flag = 1
            else:
                pass
        if(flag == -1):
            delete.append(j)
    return delete


def processingStamp(df):
    arr = []
    for i in df['orderStatusTimestamps']:
        flag = 0
        for j in i:
            if j['status'] == 'PROCESSING' and flag == 0:
                t = j['timestamp']
                flag = 1
        if(flag == 0):
            t = datetime.datetime(2011, 3, 1, 10, 38, 38, 324000)
        arr.append(t)
    return arr


def readyStamp(df):
    arr = []
    for i in df['orderStatusTimestamps']:
        flag = -1
        for j in i:
            if j['status'] == 'READY_FOR_PICKUP':
                t = j['timestamp']
                flag = 1

        if(flag == -1):
            t = datetime.datetime(1997, 3, 1, 10, 38, 38, 324000)
        arr.append(t)
    return arr


def getStatusDictionary(df):
    arr = []
    for i in df['orderStatusTimestamps']:
        d = {}
        for j in i:
            if('PROCESSING' in j.keys() and j['status'] == 'PROCESSING'):
                pass
            else:
                d[j['status']] = j['timestamp']
        arr.append(d)
    return arr


def timeFormat(df):
    df['Ready'] = pd.to_datetime(df['Ready'])
    df['Processing'] = pd.to_datetime(df['Processing'])
    return df


def getCookingTime(df):
    return df['Ready'] - df['Processing']


def timeSeconds(d):
    return d.total_seconds()/60


def getTimeSeconds(df):
    df['time'] = df['time'].apply(timeSeconds)
    df['time'] = df['time'].apply(int)
    return df


def getQueue(df, TIME, mn):
    arr = []
    for i in range(df.shape[0]):
        if('PROCESSING' not in df['dictionary'].iloc[i]):
            arr.append(0)
            continue
        e = df['dictionary'].iloc[i]['PROCESSING']
        count = 0
        for j in range(i-1, -1, -1):
            d = df['dictionary'].iloc[j]
            if 'READY_FOR_PICKUP' in d.keys():
                k = 'READY_FOR_PICKUP'
                s = d[k]
            else:
                #                 print(d)
                s = e + datetime.timedelta(minutes=mn)
            if((e-s).total_seconds()/60 > TIME):
                break
            if(e < s):
                count += 1
        arr.append(count)
    df['queue'] = arr
    return df


def getDishQuantity(df):
    arr = []
    for i in range(df.shape[0]):
        ar = []
        r = df['kitchenOrderProducts'].iloc[i]
        for j in r:
            d = {}
            if 'refBundleProductId' in j:
                d[j['refBundleProductId']] = j['quantity']
                ar.append(d)
            else:
                d[j['refProductId']] = j['quantity']
                ar.append(d)
            if 'selectedBundleProductItems' in j:

                for k in j['selectedBundleProductItems']:
                    d = {}
                    if 'refProductId' in k:
                        d[k['refProductId']] = j['quantity']
                        ar.append(d)
        arr.append(ar)
    return arr


def k(d):
    if(len(d) == 0):
        return ''
    r = d[0].keys()
    return list(r)


def k(d):
    if(len(d) == 0):
        return ''
    r = d[0].values()
    return list(r)


def product(a, b):
    arr = []
    for i in range(len(a)):
        for j in range(b[i]):
            arr.append(a[i])
    return arr


def dishQuan(val):
    arr = []
    for i in val:
        d = product(list(i.keys()), list(i.values()))
        for j in d:
            arr.append(j)
#     arr = np.array(arr)
#     arr = list(arr.ravel())
#     arr = np.reshape(arr, (np.product(arr.shape),))
    return list(arr)


def removeSpace(df):
    dd = []
    flag = 0
    for i in df['finalDish']:
        for j in range(len(i)):
            i[j] = i[j].lower()
            i[j] = i[j].strip()
            i[j] = i[j].replace("-", "")
            i[j] = i[j].replace("&", "and")
            i[j] = i[j].replace(")", "")
            i[j] = i[j].replace("(", "")
            i[j] = i[j].replace("+", "")
            i[j] = re.sub(' +', ' ', i[j])
            i[j] = i[j].replace(" ", "_")

        dd.append(i)
    df['removedItem'] = dd
    return df


def convertString(df):
    str = []
    s = " "
    for i in df['removedItem']:
        str.append(s.join(i))
    df['itemString'] = str
    return df


def deleteThirdParty(df):
    dele = []
    for i in df.index:
        for j in df["removedItem"].loc[i]:
            #             print(j)
            if 'third_party' in j:
                dele.append(i)
                break
    return dele


def getRemoveColumns(df):
    col = list(df.columns)
    print(col)
    c = ['online',
         'isComment',
         'isAddon',
         'driveThrough',
         'takeAway',
         'time',
         'queue',
         'Days',
         'Week_no',
         'hours',
         'Months', 'itemString', 'Date_']
    for i in c:
        col.remove(i)
    return col


def encoding(df, col):
    df = df.drop(col, axis=1)
    df['Days'] = df['Days'].map({'Sunday': 0, 'Monday': 1, 'Tuesday': 2,
                                'Wednesday': 3, 'Thursday': 4, 'Friday': 5, "Saturday": 6})
    df['driveThrough'] = df['driveThrough'].map({True: 1, False: 0})
    df['online'] = df['online'].map({True: 1, False: 0})
    df['takeAway'] = df['takeAway'].map({True: 1, False: 0})
#     df['homeDelivery'] = df['homeDelivery'].map({True:1,False:0})
    df['Months'] = df['Months'].map({'August': 7, 'September': 8, 'October': 9, 'November': 10, 'December': 11,
                                     'January': 0, 'February': 1, 'March': 2, 'April': 3, 'May': 4, 'June': 5, 'July': 6})
    cv = CountVectorizer(ngram_range=(0, 1))
    x = cv.fit_transform(df['itemString'])
    df1 = pd.DataFrame(x.todense(), columns=cv.get_feature_names())
    for c in df.columns:
        df1[c] = list(df[c])
    df1 = df1.drop('itemString', axis=1)
    return df1


def split(df):
    y = df['time']
    x = df.drop('time', axis=1)
    return x, y
# df.set_index("Date_", inplace = True)



def getMode(df, dele):
    df1 = df.drop(index=dele)
    mode = df1['time'].value_counts().index[0]
    return int(mode)


def getTimeBreak(df, dele):
    df1 = df.drop(index=dele)
    n = df1.shape[0]
    timeCounts = df1['time'].value_counts()
    df2 = pd.DataFrame(timeCounts)
    df2['percent'] = (df2['time']/n)*100
    maxTime = max(df2[df2['percent'] >= 1].index)
    return maxTime


def getisAddon(df):
    addOnArray = []
    for i in df.index:
        addOn = False
        for j in df['kitchenOrderProducts'].loc[i]:
            flag = 0
            if 'selectedBundleProductItems' in j.keys():
                flag = 1
                for k in j['selectedBundleProductItems']:
                    if(len(k['addons']) > 0):
                        addOn = True
                        flag = 2
                        break
            if(flag == 2):
                break
            if(flag == 0 and len(j['addons']) > 0):
                addOn = True
                break
        addOnArray.append(addOn)
    return addOnArray


def train_xg_boost(x_train, y_train, x_test, y_test):
    regr = XGBRegressor(base_score=0.5, booster='gbtree', colsample_bylevel=1,
                        colsample_bynode=1, colsample_bytree=0.4, enable_categorical=False,
                        gamma=0.2, gpu_id=-1, importance_type=None,
                        interaction_constraints='', learning_rate=0.01, max_delta_step=0,
                        max_depth=8, min_child_weight=5,
                        monotone_constraints='()', n_estimators=500, n_jobs=8,
                        num_parallel_tree=1, predictor='auto', random_state=0, reg_alpha=0,
                        reg_lambda=1, scale_pos_weight=1, subsample=1, tree_method='exact',
                        validate_parameters=1, verbosity=None)
    regr.fit(x_train,
             y_train,
             eval_set=[(x_train, y_train), (x_test, y_test)],
             eval_metric='rmse')
    return regr


def train_random_forest(x_train, y_train, x_test, y_test):
    regr = RandomForestRegressor(max_depth=2, random_state=0)
    regr.fit(x_train, y_train)
    return regr


def mse(regr, x_test, y_test):
    testPredict = regr.predict(x_test)
    mse1 = mean_squared_error(y_test, testPredict)
    return mse1

def reqiuiredModule():
    import sys
    import subprocess

    try:
        import sklearn
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install','sklearn'])

# implement pip as a subprocess:


def extract():
    # SHOPID = "5c3c4c07febd2d0001c433f4" # kwargs.get('SHOPID')
    # connection_url = "mongodb://localhost:27017/" # kwargs.get('connection_url')
    # client = MongoClient(connection_url)
    # db = client['qopla']
    # collection = db['kitchenOrder']
    # sDay, sMonth, sYear = 18, 4, 2020
    # eDay, eMonth, eYear = 30, 4, 2020
    # start = datetime.datetime(sYear, sMonth, sDay, 0, 0, 0, 0)
    # end = datetime.datetime(eYear, eMonth, eDay, 23, 59, 59, 381)

    # data = collection.find(
    #     {'createdAt': {'$gte': start, '$lt': end}, "shopId": SHOPID})
    # data = list(data)
    # df = pd.DataFrame(data)
    # df.to_pickle("data.pkl")
    # print("\n--------------------------------------------------------------\n")
    # print(df)
    # print("\n--------------------------------------------------------------\n")
    df = pd.read_pickle("/opt/airflow/dags/repo/data/df.pkl")
    df.to_pickle('/home/airflow/data.pkl')


def preprocess():
    df = pd.read_pickle("/home/airflow/data.pkl")
    df['createdAt'] = df['createdAt'].astype('datetime64[ns]')
    df['Date_'] = df['createdAt'].dt.strftime('%Y-%m-%d')
    df['Days'] = df['createdAt'].dt.day_name()
    df['Week_no'] = df['createdAt'].dt.isocalendar().week
    df['hours'] = df['createdAt'].dt.hour
    df['Months'] = df['createdAt'].dt.month_name()
    # df[df['status'] != 'DENIED']
    df['isComment'] = df['comment'].isnull()
    df["isAddon"] = getisAddon(df)
    delete = removeIndex(df)
    df['Processing'] = processingStamp(df)
    df['Ready'] = readyStamp(df)
    df['dictionary'] = getStatusDictionary(df)
    df = timeFormat(df)
    df['time'] = getCookingTime(df)
    df = getTimeSeconds(df)
    mode = getMode(df, delete)
    timeBreak = getTimeBreak(df, delete)
    df = df[df['time'] <= timeBreak]
    df = getQueue(df, timeBreak, mode)
    df = df.drop(index=delete)
    df['dishquantity'] = getDishQuantity(df)
    df['dish'] = df['dishquantity'].apply(k)
    df['quantity'] = df['dishquantity'].apply(k)
    df['finalDish'] = df['dishquantity'].apply(dishQuan)
    df = removeSpace(df)
    df = convertString(df)
    dele = deleteThirdParty(df)
    df = df.drop(index=dele)
    col = getRemoveColumns(df)
    df1 = encoding(df, col)
    X, Y = split(df1)
    X.to_pickle("/home/airflow/X.pkl")
    Y.to_pickle("/home/airflow/Y.pkl")
    print("\n--------------------------------------------------------------\n")
    print(X)
    print("\n--------------------------------------------------------------\n")
    print(Y)
    print("\n--------------------------------------------------------------\n")


def train_xg(**context):
    X = pd.read_pickle("/home/airflow/X.pkl")
    Y = pd.read_pickle("/home/airflow/Y.pkl")
    X = X.drop(["Date_"], axis=1)
    x_train, x_test, y_train, y_test = train_test_split(
        X, Y, test_size=0.2, random_state=0)
    print(x_train, y_train)
    regr = train_random_forest(x_train, y_train, x_test, y_test) # TODO: need to change it to xg_boost
    resMse = mse(regr, x_test, y_test)
    context['ti'].xcom_push(key='xg_mse', value=resMse)
    print("\n--------------------------------------------------------------\n")
    print(resMse)
    print("\n--------------------------------------------------------------\n")


def train_rf(**context):
    X = pd.read_pickle("/home/airflow/X.pkl")
    Y = pd.read_pickle("/home/airflow/Y.pkl")
    X = X.drop(["Date_"], axis=1)
    x_train, x_test, y_train, y_test = train_test_split(
        X, Y, test_size=0.2, random_state=0)
    print(x_train, y_train)
    regr = train_random_forest(x_train, y_train, x_test, y_test)
    resMse = mse(regr, x_test, y_test)
    context['ti'].xcom_push(key='rf_mse', value=resMse)
    print("\n--------------------------------------------------------------\n")
    print(resMse)
    print("\n--------------------------------------------------------------\n")


def compare_results(**context):
    rf_mse = context.get('ti').xcom_pull(key='rf_mse')
    xg_mse = context.get('ti').xcom_pull(key='xg_mse')
    
    if rf_mse < xg_mse:
        result = f"RandomForest is better by {xg_mse - rf_mse}"
    else:
        result = f"XGBoost is better by {rf_mse - xg_mse}"
    context['ti'].xcom_push(key='result', value=result)
    print("\n--------------------------------------------------------------\n")
    print(result)
    print("\n--------------------------------------------------------------\n")


# def send_mail(**context):
#     result = context.get('ti').xcom_pull(key='result')
#     message = Mail(
#     from_email='balasubhayu99@gmail.com',
#     to_emails='subhayu.kumar@fiftyfivetech.io',
#     subject='Your ML model has been trained',
#     html_content=f'<strong>{result}</strong>')
#     try:
#         sg = SendGridAPIClient('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')
#         response = sg.send(message)
#         print(response.status_code)
#         print(response.body)
#         print(response.headers)
#     except Exception as e:
#         print(e.message)
def send_mail(**context):
    pass

with DAG(
    dag_id="ml_etl",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "start_date": datetime.datetime(2022, 6, 25)
    },
    catchup=False,
    tags=['ml', 'etl']) as f:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )

    preprocess_data = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess,
    )

    train_model_xg = PythonOperator(
        task_id="train_model_xg",
        python_callable=train_xg,
    )

    train_model_rf = PythonOperator(
        task_id="train_model_rf",
        python_callable=train_rf,
    )

    compare_res = PythonOperator(
        task_id="compare_res",
        python_callable=compare_results,
    )

    mail_sender = PythonOperator(
        task_id="mail_sender",
        python_callable=send_mail,
    )

    extract_data >> preprocess_data >> [train_model_xg, train_model_rf] >> compare_res >> mail_sender
