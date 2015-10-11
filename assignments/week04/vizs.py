
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')


# ### Getting the observations for the selected stations

# In[2]:

p = Path('../../data/ncdc')
observationsCSV = p.joinpath('observations_vlc.csv')
print ('Reading observations CSV')
dfObs = pd.read_csv(str(observationsCSV),index_col=0)
print ("{:,} observations".format(len(dfObs)))


# In[3]:

dfObs.head()


# ### Data management operations

# Generate an index using the id station and the date

# In[4]:

def getId(stn,wban):
    try:
        istn = int(stn)
        iwban = int(wban)
        return "{:0>6}-{:0>5}".format(istn,iwban)
    except ValueError:
        print("{}/{}".format(stn,wban))
        
def getStationByStnWban(stn,wban):
    try:
        koppen = scdfc.loc[getId(stn,wban)].koppen
    except KeyError:
        koppen = None
    return koppen   
def getDateTimeFromRow(row):
    try:
        iyear = int(row.year)
        imonth = int("{:0>4}".format(row.monthday)[0:2])
        iday = int("{:0>4}".format(row.monthday)[2:4])
        return  datetime(iyear,imonth,iday)
    except ValueError:
        return np.nan

dfObs['date'] = dfObs.apply(lambda row : getDateTimeFromRow(row),axis=1)
dfObs.set_index(['date'],inplace=True)


# The `frshtt` column needs to be padded with zeros to get all the flags in the correct place. Then is possible to get the occurrence of different weather conditions

# In[5]:

dfObs['frshtt']  = dfObs.apply(lambda row: "{:0>6}".format(row.frshtt),axis=1)
dfObs['fog']     = dfObs['frshtt'].apply(lambda row: row[0:1]=='1')
dfObs['rain']    = dfObs['frshtt'].apply(lambda row: row[1:2]=='1')
dfObs['snow']    = dfObs['frshtt'].apply(lambda row: row[2:3]=='1')
dfObs['hail']    = dfObs['frshtt'].apply(lambda row: row[3:4]=='1')
dfObs['thunder'] = dfObs['frshtt'].apply(lambda row: row[4:5]=='1')
dfObs['tornado'] = dfObs['frshtt'].apply(lambda row: row[5:6]=='1')


# Recode the temperatures columns, replacing the NaN values and afterwards as numerics

# In[6]:

dfObs['tempC'] = dfObs['temp'].replace('99.9', np.nan)
dfObs['maxC']  = dfObs['max'].replace('99.9', np.nan)
dfObs['minC']  = dfObs['min'].replace('99.9', np.nan)

dfObs['tempC'] = pd.to_numeric(dfObs['tempC'])
dfObs['maxC']  = pd.to_numeric(dfObs['maxC'])
dfObs['minC']  = pd.to_numeric( dfObs['minC']) 

def FtoC(f):
    return (f-32)*5/9

dfObs['tempC']= dfObs['tempC'].apply(lambda temp: FtoC(temp))
dfObs['maxC'] = dfObs['maxC'].apply(lambda temp: FtoC(temp))
dfObs['minC'] = dfObs['minC'].apply(lambda temp: FtoC(temp))


# In[7]:

dfObs.head()


# ## Univariate visualization

# In[8]:

get_ipython().magic('matplotlib inline')
df = dfObs.copy()


# ### Quantitative variables

# In[9]:

df[['tempC','maxC','minC']].describe()


# In[10]:

sns.distplot(df["tempC"].dropna(), kde=False);
plt.xlabel('Temperature (ºC)')
plt.title('Mean temperature')


# In[11]:

sns.distplot(df["maxC"].dropna(), kde=False);
plt.xlabel('Temperature (ºC)')
plt.title('Max temperature')


# In[12]:

sns.distplot(df["minC"].dropna(), kde=False);
plt.xlabel('Temperature (ºC)')
plt.title('Min temperature')


# Plotting the three variables together

# In[13]:

sns.kdeplot(df.tempC, label="Mean")
sns.kdeplot(df.maxC, label="Max")
sns.kdeplot(df.minC, label="Min")
plt.legend();
plt.xlabel('Temperature (ºC)')
plt.title('Valencia station temperatures')


# ### Cualitative variables

# Our quantitative variables are all `True/False` so they are categorical by definition

# In[15]:

sns.countplot(x="rain", data=df);
plt.xlabel('It rained?')
plt.title('Raining days')


# In[16]:

sns.countplot(x="fog", data=df);
plt.xlabel('Fog recorded?')
plt.title('Foggy days')


# ## Bivariate visualizations

# Let's compare temperature and rainy days (quantitative to cualitative)

# In[17]:

sns.factorplot(x="rain", y="tempC", data=df, kind="bar", ci=None)
plt.xlabel('It rained?')
plt.ylabel('Mean temperature')
plt.title('Rainy days accross temperatures')


# What about pressure and rainy days?

# In[18]:

sns.factorplot(x="rain", y="slp", data=df, kind="bar", ci=None)
plt.xlabel('It rained?')
plt.ylabel('Mean pressure')
plt.title('Rainy days against sea level pressure')


# In[19]:

sns.factorplot(x="rain", y="visib", data=df, kind="bar", ci=None)
plt.xlabel('It rained?')
plt.ylabel('Visibility (miles)')
plt.title('Rainy days against visibility (in miles)')


# Let's compare sea level presure and temperatures using a scatter plot

# In[20]:

sns.regplot(x="tempC", y="slp", data=df)
plt.xlabel('Mean temperature')
plt.ylabel('Sea Level Pressure')
plt.title('Scatterplot for temperatures aganist sea level pressure')


# What about using the measured precipitations?

# In[21]:

sns.regplot(x="prcp", y="slp", data=df[(df.prcp>0.1)])
plt.xlabel('Precipitation')
plt.ylabel('Sea Level Pressure')
plt.title('Scatterplot for precipitation aganist sea level pressure')


# In[22]:

sns.regplot(x="prcp", y="visib", data=df[(df.prcp>0.1)])
plt.xlabel('Precipitation')
plt.ylabel('Sea Level Pressure')
plt.title('Scatterplot for precipitation aganist sea level pressure')

