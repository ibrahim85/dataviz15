
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib
from IPython.display import HTML
import requests
from datetime import datetime


#### Import stations CSV

# My first step is to find interesting stations and one of the requirements is to find some that have a good time range coverage so I will evaluate the files downloaded.

# Reading the stations coordinates CSV

# In[2]:

stations = pd.read_csv(
    '../../data/ncdc/ish-history.csv',
    skiprows=1, 
    names=['usaf','wban','stname','ctry','fips','state','call','lat' ,'lon' ,'elev'],
    dtype=object)
print (len(stations))
stations[:3]


# Get correct coordinates dividing the lat/lon by 1000 and the elevation by 10. Then generate a unique index and a column that specficies if the id is from a USAF station or from WBAN. Finally drop any station that doesn't have a location to use.

# In[3]:

stations['lat'] = stations.lat.apply(lambda lat: float(lat)/1000)
stations['lon'] = stations.lon.apply(lambda lon: float(lon)/1000)
stations['elev'] = stations.elev.apply(lambda e: float(e)/10.0)


# In[4]:

stations['id']     = stations.apply(lambda row : "{}-{}".format(row.usaf,row.wban),axis=1)
stations.set_index(['id'],inplace=True)
stations.dropna(how='any',subset=['lat','lon'],inplace=True)
stations.head()


# In[5]:

stations.tail()


#### Read CSV of observations

# Instantiate the path where the NCDC data has been downloaded

# In[6]:

p = Path('../../data/ncdc')


# Check if we have already the stations CSV or generate it from the files. To generate the CSV the name file will be inspected but also a full read of the file will be performed in order to count the number of observations on every station.

# In[7]:

gsodCSV = p.joinpath('gsod.csv')
if not gsodCSV.exists():
    ops = p.joinpath('raw').joinpath('gsod').glob('**/*.op')
    agsod = []

    for op in ops:
        try:
            data = op.name.replace('.op','').split('-')
            data.append(sum(1 for line in op.open( encoding='utf8' ))-1)
            agsod.append(data)
        except UnicodeDecodeError:
          print (op.absolute())
    dfGsod = pd.DataFrame(data = agsod, columns=['usaf','wban','year','obs'])
    dfGsod['id'] = dfGsod.apply(lambda row: "{}-{}".format(row.usaf,row.wban) ,axis=1)
    dfGsod = dfGsod.set_index(['id'])
    dfGsod[['year','obs']].to_csv(str(gsodCSV))



# In[8]:

print ('Reading existing stations per year CSV')
dfGsod = pd.read_csv(str(gsodCSV),index_col=['id'])
print ("{:,} station files".format(len(dfGsod)))
print ("{:,} observations".format(dfGsod.obs.sum()))
dfGsod.head()


#### Year statistics per station

# Now study this dataframe, grouping by id and see the total years recorded, max and min

# In[9]:

year_groups = dfGsod[['year']].groupby(level=0)
year_count  = year_groups.count()
year_max    = year_groups.max()
year_min    = year_groups.min()

years = pd.concat([year_count,year_max,year_min],axis=1)
years.columns = ['count','max','min']
years.head()


#### Count observations per station

# In[10]:

obs_groups = dfGsod[['obs']].groupby(level=0)
obs_count = obs_groups.sum()
obs_count.head()


#### Join stations and observations statistics

# Now we can check if the indexes of both data frames are unique and then join them to retreive only the stations with observations

# In[11]:

stations.index.is_unique and years.index.is_unique and obs_count.index.is_unique


# In[12]:

scdf = pd.concat([stations,years,obs_count],axis=1,join='inner')
scdf.head()


# Finally we can study this dataset and filter the appropriate stations for our study on this first iteration

# In[13]:

scdf.to_csv('stations.csv')


#### Get preferred stations

# Next step is done at [CartoDB](http://cartodb.com) an analysis and mapping service. From the exported `stations.csv`, I've created a map of the 2 stations with more years of observations for every [Köppen-Geiger](http://koeppen-geiger.vu-wien.ac.at/shifts.htm) classification. The map is interactive, you can zoom in and click on stations to check it's attributions.

# In[14]:

HTML('<iframe width="100%" height="520" frameborder="0" src="https://team.cartodb.com/u/jsanz/viz/c9d103fe-6ab1-11e5-b45b-0e674067d321/embed_map" allowfullscreen webkitallowfullscreen mozallowfullscreen oallowfullscreen msallowfullscreen></iframe>')


# To get those stations back to this notebook I will use CartoDB SQL API so I will execute this SQL using the CSV format so it can be read directly into a Pandas DataFrame.
# 
# ```
# WITH ranked AS (
#   SELECT 
#   s.id, r.gridcode,
#   rank() over (partition by r.gridcode order by s.count desc, s.elev asc) pos
#   FROM stations s 
#   JOIN climate_regions r ON ST_Intersects(s.the_geom,r.the_geom)
# ), filtered as (
#   SELECT 
#   r.id,k.koppen,
#   rank() over (partition by r.id order by k.koppen) pos
#   FROM ranked r
#   JOIN koppen k ON r.gridcode = k.gridcode
#   WHERE pos < 3
# ) 
# SELECT id,koppen FROM filtered WHERE POS = 1
# ```

# In[15]:

query = 'https://jsanz.cartodb.com/api/v1/sql?format=csv&q=WITH+ranked+AS+(%0A++SELECT+%0A++s.id,+r.gridcode,%0A++rank()+over+(partition+by+r.gridcode+order+by+s.count+desc,+s.elev+asc)+pos%0A++FROM+stations+s+%0A++JOIN+climate_regions+r+ON+ST_Intersects(s.the_geom,r.the_geom)%0A),+filtered+as+(%0A++SELECT+%0A++r.id,k.koppen%0A++,rank()+over+(partition+by+r.id+order+by+k.koppen)+pos%0A++FROM+ranked+r%0A++JOIN+koppen+k+ON+r.gridcode+%3D+k.gridcode%0A++WHERE+pos+%3C+3+%0A)+%0ASELECT+id,koppen%0AFROM+filtered+WHERE+POS+%3D+1'
selections = pd.read_csv(query,index_col=['id'])


# Check if the index of the imported dataframe is unique and then join it with our stations dataset. This join will keep only data on both data frames using the parameter `join='inner'`.

# In[16]:

selections.index.is_unique


# In[17]:

scdfc = pd.concat([scdf,selections],axis=1,join='inner')


#### Getting the observations for the selected stations

# Filter the observations dataframe (11M records) by the selected stations and generate the files to read from the observations folder

# In[18]:

files_to_read = pd.merge(left=dfGsod,right=scdfc,left_index=True,right_index=True,)[['year']]
files_to_read['id'] = files_to_read.index
files_to_read['path'] = files_to_read.apply(lambda row: Path('../../data/ncdc/raw/gsod').joinpath("{0}/{1}-{0}.op".format(row.year,row.id)),axis=1)
print ("{} files to read".format(len(files_to_read)))


# Read the files defined previously and store the results on a new big data frame and CSV adding the *Köppen* classification

# In[19]:

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


# In[ ]:

observationsCSV = p.joinpath('observations.csv')
if not observationsCSV.exists():
    i = 0
    acc = 0
    for index,row in files_to_read.iterrows():
        path = row['path']
        if path.exists():
            dfObsTemp = pd.read_fwf(
                    str(path),
                    colspecs=[(0,7),(7,13),(14,18),(18,22),(25,30),(31,33),(35,41),
                              (42,44),(46,52),(53,55),(57,63),(64,66),(68,73),(74,76),
                              (78,84),(84,86),(88,93),(95,100),(102,108),(108,109),
                              (110,116),(116,117),(118,123),(123,124),(125,130),(132,138)],
                    skiprows=1,
                    names=['stn','wban','year','monthday','temp','temp_count',
                           'dewp','dewp_count','slp','slp_count','stp','stp_count',
                           'visib','visib_count','wsdp','wsdp_count','mxspd',
                           'gust','max','max_flag','min','min_flag','prcp','prc_flag','sndp','frshtt']
                    )
            dfObsTemp['koppen'] = dfObsTemp.apply(lambda row: getStationByStnWban(row.stn,row.wban),axis=1)
            dfObsTemp.to_csv(str(observationsCSV),mode='a')

            i += 1
            acc += len (dfObsTemp)

            if i % 1000 == 0:
                print("{:>8} obs".format(acc))


# In[ ]:

print ('Reading observations CSV')
dfObs = pd.read_csv(str(observationsCSV),index_col=0)
dfObs = dfObs[(dfObs.stn != 'stn')]
print ("{:,} observations".format(len(dfObs)))
dfObs.head()


#### Performing data management operations on the dataset

# Now that we have the raw dataset, we can start doing management operations. First generate a copy dataframe with only the columns we are interested in.

# In[ ]:

dfObs2 = dfObs.copy()[['stn','wban','year','monthday','temp','max','min','frshtt','koppen']]


##### Management

# Generate an index using the id station and the date

# In[ ]:

def getDateTimeFromRow(row):
    try:
        iyear = int(row.year)
        imonth = int("{:0>4}".format(row.monthday)[0:2])
        iday = int("{:0>4}".format(row.monthday)[2:4])
        return  datetime(iyear,imonth,iday)
    except ValueError:
        return np.nan

dfObs2['id']   = dfObs2.apply(lambda row: getId(row.stn,row.wban) ,axis=1)
dfObs2['date'] = dfObs2.apply(lambda row : getDateTimeFromRow(row),axis=1)
dfObs2.set_index(['id','date'],inplace=True)


# The `frshtt` column needs to be padded with zeros to get all the flags in the correct place. Then is possible to get the occurrence of different weather conditions

# In[ ]:

dfObs2['frshtt']  = dfObs2.apply(lambda row: "{:0>6}".format(row.frshtt),axis=1)
dfObs2['fog']     = dfObs2['frshtt'].apply(lambda row: row[0:1]=='1')
dfObs2['rain']    = dfObs2['frshtt'].apply(lambda row: row[1:2]=='1')
dfObs2['snow']    = dfObs2['frshtt'].apply(lambda row: row[2:3]=='1')
dfObs2['hail']    = dfObs2['frshtt'].apply(lambda row: row[3:4]=='1')
dfObs2['thunder'] = dfObs2['frshtt'].apply(lambda row: row[4:5]=='1')
dfObs2['tornado'] = dfObs2['frshtt'].apply(lambda row: row[5:6]=='1')


# Recode the temperatures columns, replacing the NaN values and afterwards as numerics

# In[ ]:

dfObs2['tempC'] = dfObs2['temp'].replace('99.9', np.nan)
dfObs2['maxC']  = dfObs2['max'].replace('99.9', np.nan)
dfObs2['minC']  = dfObs2['min'].replace('99.9', np.nan)

dfObs2['tempC'] = dfObs2['tempC'].convert_objects(convert_numeric=True) / 10
dfObs2['maxC']  = dfObs2['maxC'].convert_objects(convert_numeric=True) / 10
dfObs2['minC']  = dfObs2['minC'].convert_objects(convert_numeric=True) / 10


##### Frequency tables

# Frequency tables for koppen, thunders, tornados that are categorized

# In[ ]:

dfObs.koppen.value_counts(normalize=True)*100


# In[ ]:

dfObs2.tornado.value_counts(normalize=True)*100


# In[ ]:

dfObs2.thunder.value_counts(normalize=True)*100


# Categorize the temperatures by quantiles and then make the frequency table to confirm the categorization

# In[ ]:

dfObs2['temp4']=pd.qcut(dfObs2.tempC, 4, labels=["1=0%tile","2=25%tile","3=50%tile","4=75%tile"])
dfObs2['temp4'].value_counts(normalize=True)*100


# And to get the cuts, we can group by the categorization column and get the max value for every group

# In[ ]:

dfObs2[['tempC']].max()


# In[ ]:

group_by_year= dfObs2[['year','tempC','koppen']].groupby(['year','koppen'])


# In[ ]:

group_by_year.agg([np.max,np.std])

