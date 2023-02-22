from pymatch.Matcher import Matcher



import pyspark
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

#pandas 
import pandas as pd



# setup session 
spark = SparkSession.builder.getOrCreate()
pd.set_option('display.max_rows', 100)
# sns.set(rc = {'figure.figsize':(15,8)})
# sns.set_style(style='white')

## conversion between pyspark and pandas dataframe
spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")


df_main = spark.read.format('parquet').option('header','true').load("dbfs:/mnt/bastore/modelling/small/BE_Freebook/data_analysis")
df_pd = df_main.toPandas()

# remove NULL values 
df_pd = df_pd[df_pd['SubSen_Max_M1_1']!='NULL']

#change comma's to dots 
payments = ['PayCInfl_Sum_M1_1','PayCInfl_Sum_M1_3', 'PayCInfl_Sum_M1_6', 'PayCInfl_Sum_Y1_1']
for i in payments:
    df_pd[i]= df_pd[i].apply(lambda x: x.replace(',', '.'))


df_pd_test = df_pd[['FK_Consumer', 'MonthRank', 'SubSen_Max_M1_1',
       'SubSen_Max_Inf', 'SubA_Bool_M1_1', 'SubFJ_Bool_M1_1', 'SubH_Bool_M1_1',
       'SubW_Bool_M1_1', 'SubF_Bool_M1_1', 'SubT_Bool_M1_1',
       'SubBreak_Cnt_Inf', 'Ret_Cnt_Y1_4',
      'PayCInfl_Sum_M1_1','PayCInfl_Sum_M1_3', 'PayCInfl_Sum_M1_6', 'PayCInfl_Sum_Y1_1',
       'PayFreq_Max_M1_12', 'PayLast_Diff_M', 'RetMonthsTillEndRet',
       'RetPctFullPrice', 'NrMonthsPreviousSubscriptionF',
       'NrMonthsPreviousSubscriptionH', 
       'NrMonthsPreviousSubscriptionW', 'NrMonthsPreviousSubscriptionHT',
       'NrMonthsPreviousUpsell', 'NrMonthsPreviousUpsellF',
       'NrMonthsPreviousUpsellH', 
       'NrMonthsPreviousUpsellW', 'NrMonthsPreviousUpsellHT', 'NrPrevUpsell',
       'NrPrevUpsellF', 'NrPrevUpsellH',  'NrPrevUpsellW',
       'NrPrevUpsellHT', 'WebVisit_Cnt_M1_6', 'WebVisit_Cnt_Y1_1',
       'WebVisit_Bool_M1_6', 'WebComplain_Bool_M1_6',
       'WebProductCompare_Bool_M1_6', 'WebServiceCompare_Bool_M1_6',
       'WebCompare_Bool_M1_6', 'WebPA_Bool_M1_6', 'WebCom_Bool_M1_6',
       'PASIn_Bool_M1_12', 'PASInCall_Bool_M1_12', 'Freeb_Bool_M1_12',
       'CC2In_Bool_M1_1', 'NewsLFOpen_Cnt_M1_12',
       'NewsLHOpen_Cnt_M1_12', 'NewsLFClicked_Cnt_M1_12',
       'NewsLHClicked_Cnt_M1_12', 'churned',
     ]].astype(float)

df_pd.info()

test= df_pd_test[df_pd_test['Freeb_Bool_M1_12'] == 1 ][0:5000]
control =  df_pd_test[df_pd_test['Freeb_Bool_M1_12'] != 1 ][0:5000]



m = Matcher(control= control, test=test, yvar='Freeb_Bool_M1_12')

m.fit_scores(balance=True, nmodels=20)
GLM()

self.y, self.X = patsy.dmatrices('{} ~ {}'.format(self.yvar_escaped, '+'.join(self.xvars_escaped)),
                                         data=self.data, return_type='dataframe')


y, X = patsy.dmatrices('{} ~ {}'.format(yvar_escaped, '+'.join(self.xvars_escaped)),
                                         data=self.data, return_type='dataframe')






GLM()





















m.predict_scores()
plt.show()
m.plot_scores()