import io
import pytz
import pandas as pd
import numpy as np
import requests
from datetime import timedelta
from distance import distance

'''Distance code from geopy, inserted into api_pull.py to avoid
downloading another package'''

class weatherMeta:
    '''
    Instantiates a parent class with parameters for pulling api weather data.
    
    Parameters
    ----------
    start : timestamp or string
        The start date to pull the temperature data with.

    end : timestamp or string
        The end date to pull the temperature data with.

    lat : float
        Latitude of interest.

    lon : float
        Longitude of interest.
 
    '''
    def __init__(self, start, end, lat, lon):
        self.lat(lat)
        self.lon(lon)
        self.start(start)
        self.end(end)

    def start(self, val):
        if val.__class__ != pd._libs.tslibs.timestamps.Timestamp:
            self._start = pd.to_datetime(val)
        elif val.__class__ == str:
            self._start = val
        else:
            raise ValueError("Please input string or timestamp.")

    def end(self, val):
        if val.__class__ != pd._libs.tslibs.timestamps.Timestamp:
            self._end = pd.to_datetime(val)
        elif val.__class__ == str:
            self._end = val
        else:
            raise ValueError("Please input string or timestamp.")

    def lat(self, val):
        if val > 180 or val < -180:
            raise ValueError("Please enter latitude between \
                                -90 to 90 or -180 to 180")
        self._lat = val

    def lon(self, val):
        if val > 360 or val < -360:
            raise ValueError("Please enter longitude between \
                                -180 to 180 or -360 to 360")
        self._lon = val

class weatherPullASOS(weatherMeta):
    '''
    Get temperature and heat index from specified station
    date range is input via (start, end) as datetime or pandas timestamp types
    
    Parameters
    ----------
    start : timestamp or string
        The start date to pull the temperature data with.

    end : timestamp or string
        The end date to pull the temperature data with.

    lat : float
        Latitude of interest.

    lon : float
        Longitude of interest.
        
    fix_missing : bool
        Added as a flag to check for different ASOS stations 
        if data is missing (when True).
    
    loc_len : int
        Sets the length of the closest ASOS station locations;
        e.g., if 3, the 3 ordered closest ASOS stations.

    Notes
    -----
    This transform is ideally instantiated at time of event and fed back into a
    database as a timeseries. Get data via the get_data() function.
    
    '''
    def __init__(self, start, end, lat, lon, fix_missing=False, loc_len=1):
        super().__init__(start, end, lat, lon)
        self.loc = find_asos(lat,lon,n=10)
        self.fix_missing=fix_missing
        self.loc_len=loc_len
        if start.__class__ != pd._libs.tslibs.timestamps.Timestamp:
            self.start = pd.to_datetime(start)
            self.end = pd.to_datetime(end)
        else:
            self.start = start
            self.end = end
        if fix_missing == False:
            self.service=self.url_builder(self.start, self.end, self.loc[0])    
        else:
            self.service=self.url_builder(self.start, self.end, self.loc[self.loc_len])  
        self.lat = lat
        self.lon = lon

    def url_builder(self, start, end, loc):
        SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
        service = SERVICE + "data=tmpf&data=feel&tz=America%2FNew_York&format=onlycomma&missing=null&"
        service += start.strftime('year1=%Y&month1=%m&day1=%d&')
        service += end.strftime('year2=%Y&month2=%m&day2=%d&')
        service += 'station={}&'.format(loc)
        service += 'report_type=1&report_type=2&'
        return service

    def query_api(self):
        wdata = requests.get(self.service,verify=False).content
        rawData = pd.read_csv(io.StringIO(wdata.decode('utf-8')))
        rawData['valid'] = pd.to_datetime(rawData['valid'])
        rawData.index = rawData['valid']
        
        rawData = rawData[~rawData.index.duplicated(keep='last')]
        return rawData

    def get_data(self, check_record=False, interval='15T', verbose=False):
        '''Returns data given query parameters. Interval determines interp
        resolution (string).'''
        rawData = self.query_api()
        n = 0
        rdi = len(rawData.index)
        if check_record == False:
            while rdi < 1000:
                if verbose == True:
                    print(f'{self.loc[n]} returned len {rdi}')
                self.service = self.url_builder(self.start, self.end, self.loc[n])
                rawData = self.query_api()
                rdi = len(rawData.index)
                n += 1
        else:
            while rdi < 10:
                if verbose == True:
                    print(f'{self.loc[n]} returned len {rdi}')
                self.service = self.url_builder(self.start, self.end, self.loc[n])
                rawData = self.query_api()
                rdi = len(rawData.index)
                n += 1            
        while rawData.index[0].date() > self.start.date():
            self.service = self.url_builder(self.start, self.end, self.loc[n])
            rawData = self.query_api()
            n += 1
        interpolated_data = time_interp(rawData,interval=interval)
        self.data = interpolated_data[['tmpf','feel']].copy()
        self.data.loc[:,'tmpf'] = pd.to_numeric(self.data['tmpf'],
                                                errors='coerce').interpolate(method='from_derivatives')
        self.data.loc[:,'feel'] = pd.to_numeric(self.data['feel'],
                                                errors='coerce').interpolate(method='from_derivatives')
        return self.data

def find_asos(lat_point, lon_point, n=2):
    '''Finds and sorts NY asos stations based on input lats and lons.
    Choose number of stations to return with n (default 2).'''
    service = 'https://mesonet.agron.iastate.edu/sites/networks.php?network=NY_ASOS&format=csv&nohtml=on'
    wdata = requests.get(service, verify=False).content
    asos_stations = pd.read_csv(io.StringIO(wdata.decode('utf-8')))
    dist_list = []
    for lat, lon in zip(asos_stations['lat'],asos_stations['lon']):
        dist_list.append(distance((lat_point, lon_point), (lat, lon)).km)
    asos_stations['dist'] = dist_list
    return asos_stations['stid'].loc[asos_stations['dist'].nsmallest(n).index].values


class weatherGovPull(weatherMeta):
    def __init__(self, start, end, lat, lon):
        super().__init__(start, end, lat, lon)
        wgov_link = self.weathergov_api_link()
        self.data = self.weathergov_pull_forecast(wgov_link)


    def weathergov_api_link(self):
        '''Returns a formatted link for the weather.gov api with lat and lon'''
        service = f"https://api.weather.gov/points/{self.lat},{self.lon}/forecast/hourly"
        return service

    
    def weathergov_pull_forecast(self, link, var: str='temperature'):
        '''Pulls forecast from weather.gov. var is temperature or windSpeed.'''
        resp = requests.get(link, verify=False).json()
        if var == 'windSpeed':
            output = np.array([[np.datetime64(n['startTime']),n[var]] for n in resp['properties']['periods']])
            output[:,1] = [int(''.join(list(filter(str.isdigit, n)))) for n in output[:,1]]
            return output
        return np.array([[np.datetime64(n['startTime']),n[var]] for n in resp['properties']['periods']])

def time_interp(df,interval='5T'):
    '''Interpolates DataFrame to quarter hour, needs DatetimeIndex as index'''
    oidx = df.index
    nidx = pd.date_range(oidx.min(), oidx.max(), freq=interval)
    df = df.apply(pd.to_numeric,errors='coerce')
    interpolated_data = df.reindex(oidx.union(nidx)).interpolate('from_derivatives').reindex(nidx)
    interpolated_data.index = interpolated_data.index.round(interval)

    return interpolated_data

def utc_to_est(data):
    '''Converts datetimeindex from utc to est depending on dst'''
    zonename = 'America/New_York'
    tz = pytz.timezone(zonename)
    now = pytz.utc.localize(data.index[0])
    if now.astimezone(tz).dst() != timedelta(0):
        data.index = data.index - pd.DateOffset(hours=4)
    else:
        data.index = data.index - pd.DateOffset(hours=5)
    return data
    