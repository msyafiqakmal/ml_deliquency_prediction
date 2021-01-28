import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier,LogisticRegression,GBTClassifier,RandomForestClassifier
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import PCA as PCAml
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StandardScaler

engine = create_engine('postgresql://<db engine here>')

spark = SparkSession.builder.getOrCreate()
MAIN_DIRECTORY = os.getcwd()
file_path =MAIN_DIRECTORY+"/final/final_dataframe.csv"
file_rus_path =MAIN_DIRECTORY+"/imbalanced/rus.csv"
file_ros_path =MAIN_DIRECTORY+"/imbalanced/ros.csv"
file_rsm_path =MAIN_DIRECTORY+"/imbalanced/rsm.csv"

final_df = spark.read.format("csv").option("header","true").option('inferSchema','true').load(file_path)
rus_df = spark.read.format("csv").option("header","true").option('inferSchema','true').load(file_rus_path)
ros_df = spark.read.format("csv").option("header","true").option('inferSchema','true').load(file_ros_path)
rsm_df = spark.read.format("csv").option("header","true").option('inferSchema','true').load(file_rsm_path)

ml_columns = final_df.columns
ml_columns.remove('Defaulter')

vecA_main = VectorAssembler(inputCols=ml_columns, outputCol="Output")

df_main = vecA_main.transform(final_df)
df_rus = vecA_main.transform(rus_df)
df_ros = vecA_main.transform(ros_df)
df_rsm = vecA_main.transform(rsm_df)

pca =PCAml(k=2, inputCol='Output', outputCol="PCAOutput")
model =pca.fit(df_main)
df_pca = model.transform(df_main)

datasets = [df_pca,df_main,df_rus,df_ros,df_rsm]
datasets_name = ['df_pca','df_main','df_rus','df_ros','df_rsm']
trainsets = []
testsets = []
for dataset in datasets:
    traintest = dataset.randomSplit([0.70,0.30],1)
    trainsets.append(traintest[0])
    testsets.append(traintest[1])
features = ['PCAOutput','Output','Output','Output','Output']

models = [DecisionTreeClassifier,LogisticRegression,RandomForestClassifier,GBTClassifier]
models_name = ['DecisionTreeClassifier','LogisticRegression','RandomForestClassifier','GBTClassifier']

dt = DecisionTreeClassifier(featuresCol="PCAOutput", labelCol="Defaulter")
DTmodel = dt.fit(train_test_pca[0])
DT_pred_PCA=DTmodel.transform(train_test_pca[1])


def modeling(model, features, train, test):
    mod = model(featuresCol=features, labelCol="Defaulter")
    fit_mod = mod.fit(train)
    pred = fit_mod.transform(test)

    return pred


def model_score(model, dataset, predicted, labelCol):
    results = predicted.select(['prediction', labelCol])
    pred_label = results.rdd
    metrics = MulticlassMetrics(pred_label)
    cm = metrics.confusionMatrix().toArray()

    accuracy = (cm[0][0] + cm[1][1]) / cm.sum()
    precision = (cm[0][0]) / (cm[0][0] + cm[1][0])
    recall = (cm[0][0]) / (cm[0][0] + cm[0][1])
    res = {"model": model, 'dataset': dataset, 'accuracy': accuracy, 'precision': precision, 'recall': recall}
    return res

d = 0
all_results= []
for dataset in datasets:
    m =0
    for model in models:
        all_results.append(model_score(model_name[m],datasets_name[d],modeling(models[m],features[d],trainsets[d],testsets[d]),'Defaulter'))
        m=m+1
    d=d+1

def scaling(scaler,dataset,inputCol,outputCol):
    scale = scaler(inputCol=inputCol, outputCol=outputCol)
    scalerModel = scale.fit(dataset)
    scaledData = scalerModel.transform(dataset)
    return scaledData

scaler = [StandardScaler,MinMaxScaler]
scaler_name = ['StandardScaler','MinMaxScaler']
scaled_data =[]
scale_trainsets =[]
scale_testsets = []
for scale in scaler:
    df_scale = scaling(scale,df_main,'Output','ScalerOutput')
    scaled_data.append(df_scale)
    traintest = df_scale.randomSplit([0.70,0.30],1)
    scale_trainsets.append(traintest[0])
    scale_testsets.append(traintest[1])

d = 0
for scaled in scaled_data:
    m =0
    for model in models:
        all_results.append(model_score(model_name[m],scaler_name[d],modeling(models[m],'ScalerOutput',scale_trainsets[d],scale_testsets[d]),'Defaulter'))
        m=m+1
    d=d+1

df = pd.DataFrame(all_results)
df

df.to_sql('final_model_result', engine, if_exists='replace')
df.to_csv('final_model_result.csv')
