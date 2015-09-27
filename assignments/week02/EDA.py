
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib
from IPython.display import HTML
import requests

# plotting options
get_ipython().magic('matplotlib inline')
#pd.set_option('display.mpl_style', 'default') # Make the graphs a bit prettier


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

# Next step is done at [CartoDB](http://cartodb.com) an analysis and mapping service. From the exported `stations.csv`, I've created a map of the stations that have more than 40 years, are below 500 meters elevation and that intersect with the warm regions according to the [Köppen-Geiger](http://koeppen-geiger.vu-wien.ac.at/shifts.htm) classification. The map is interactive, you can zoom in and click on stations to check it's attributions.

# In[14]:

HTML('<iframe width="100%" height="520" frameborder="0" src="https://team.cartodb.com/u/jsanz/viz/bd9456f8-6530-11e5-b18a-0e0c41326911/embed_map" allowfullscreen webkitallowfullscreen mozallowfullscreen oallowfullscreen msallowfullscreen></iframe>')


# To get those stations back to this notebook I will use CartoDB SQL API so I will execute this SQL using the CSV format so it can be read directly into a Pandas DataFrame.
# 
# ```
# WITH regions AS (
#   SELECT *,
#   CASE GRIDCODE 
#     WHEN 31 THEN 'Cfa'
#     WHEN 32 THEN 'Cfb'
#     WHEN 33 THEN 'Cfc'
#     WHEN 34 THEN 'Csa'
#     WHEN 35 THEN 'Csb'
#     WHEN 36 THEN 'Csc'
#     ELSE 'NA' 
#   END cat
#   FROM climate_regions 
#   WHERE gridcode >= 31 and gridcode <= 36
# ), wstations AS ( 
#   SELECT s.id, r.cat
#   FROM jsanz.stations s 
#   JOIN regions r ON ST_Intersects(s.the_geom,r.the_geom)
#   WHERE count >= 40 AND elev < 500
# )
#   select id, string_agg(cat,', ') from wstations group by id
# ```

# In[15]:

query = 'https://jsanz.cartodb.com/api/v1/sql?format=csv&q=WITH+regions+AS+(%0A++SELECT+*,%0A++CASE+GRIDCODE+%0A++++WHEN+31+THEN+%27Cfa%27%0A++++WHEN+32+THEN+%27Cfb%27%0A++++WHEN+33+THEN+%27Cfc%27%0A++++WHEN+34+THEN+%27Csa%27%0A++++WHEN+35+THEN+%27Csb%27%0A++++WHEN+36+THEN+%27Csc%27%0A++++ELSE+%27NA%27+%0A++END+cat%0A++FROM+climate_regions+%0A++WHERE+gridcode+>%3D+31+and+gridcode+<%3D+36%0A),+wstations+AS+(+%0A++SELECT+s.id,+r.cat%0A++FROM+jsanz.stations+s+%0A++JOIN+regions+r+ON+ST_Intersects(s.the_geom,r.the_geom)%0A++WHERE+count+>%3D+40+AND+elev+<+500%0A)%0A++select+id,+string_agg(cat,%27,+%27)+cat+from+wstations+group+by+id'
selections = pd.read_csv(query,index_col=['id'])
selections.head()


# Check if the index of the imported dataframe is unique and then join it with our stations dataset. This join will keep only data on both data frames using the parameter `join='inner'`.

# In[16]:

selections.index.is_unique


# In[17]:

scdfc = pd.concat([scdf,selections],axis=1,join='inner')
scdfc.head()


#### Descriptors for the preferred stations data frame

# **Note**: This is the formal answer for this week assignment, all the previous work was meant to have this dataset ready to work with it. Next weeks I will load the real observations data and do more coding with it but as for now, this is enough for the tasks asked.

# Frenquency table for the selected stations by country and filtering those with more than **20** stations.

# In[18]:

scdfc_group_ctry = scdfc.groupby(['ctry']).size()
scdfc_group_ctry[scdfc_group_ctry>20]


# Frequency table for the climate categories. Some climate areas overlapped, that's why there are three records with two assigned categories.

# In[19]:

scdfc_group_cat = scdfc.groupby(['cat']).size()
scdfc_group_cat


# Frequency table for the number of years recorded.

# In[20]:

scdfc_group_count = scdfc.groupby(['count']).size()
scdfc_group_count

