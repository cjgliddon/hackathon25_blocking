# Extract ERA5 winds associated with tracks passing through Boston
# %%
import pandas as pd, numpy as np, xarray as xr
from joblib import Parallel, delayed
import pathlib
extract=True

# %%
boston_bbox_track = [288, 41.25, 290, 43.25] # west, south, east, north of 2 degree x 2 degree distance from Boston
boston_bbox_wind = [288.55, 41.85, 289.55, 42.85] # west, south, east, north of 1 degree x 1 degree box surrounding Boston

bbox = boston_bbox_track
bbox_wind = boston_bbox_wind

# %%
pathlib.Path('winds_1hrly_era5_era5track/boston/').mkdir(parents=True, exist_ok=True)

# %%
tracks = pd.read_csv('tracks_1hrly/ERA5_TempestExtremes_XTC_Boston.csv')

# %%
def extend_track(boolean_list):
    for i, val in enumerate(boolean_list):
        if val:
            first_true_index = i
            break
    
    for i, val in reversed(list(enumerate(boolean_list))):
        if val:
            last_true_index = i
            break
    
    for i in range(max(first_true_index - 1, 0), min(last_true_index + 1 + 1, len(boolean_list))):
        boolean_list[i] = True
    
    return boolean_list

def insert_missing_time(df):
    df['datetime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
    df.set_index('datetime', inplace=True)
    complete_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='1H')
    df = df.reindex(complete_range)
    df.reset_index(inplace=True)
    df['year'] = df['index'].dt.year
    df['month'] = df['index'].dt.month
    df['day'] = df['index'].dt.day
    df['hour'] = df['index'].dt.hour
    df['track_id_local'] = int(df['track_id_local'][0])
    df['track_id_global'] = int(df['track_id_global'][0])
    df.drop(['index'], axis=1, inplace=True)
    return df

# %%
def extract_era5_wind(track, row):
    year = int(track['year'].values[row])
    month = int(track['month'].values[row])
    u_file = f"/net/flood/data/projects/liberty/data/era5/era5_u10_{year}_{month:02d}.nc"
    v_file = f"/net/flood/data/projects/liberty/data/era5/era5_v10_{year}_{month:02d}.nc"
    # u_file = '/glade/collections/rda/data/ds633.0/e5.oper.an.sfc/'+str(track['year'].values[row])+str(track['month'].values[row]).zfill(2)+'/e5.oper.an.sfc.128_165_10u.ll025sc.'+str(track['year'].values[row])+str(track['month'].values[row]).zfill(2)+'*.nc'
    # v_file = '/glade/collections/rda/data/ds633.0/e5.oper.an.sfc/'+str(track['year'].values[row])+str(track['month'].values[row]).zfill(2)+'/e5.oper.an.sfc.128_166_10v.ll025sc.'+str(track['year'].values[row])+str(track['month'].values[row]).zfill(2)+'*.nc'
 
    ds_u = xr.open_mfdataset(u_file)
    ds_v = xr.open_mfdataset(v_file)
    
    u_bbox = ds_u.sel(time=(pd.DatetimeIndex(ds_u.time).hour == track['hour'].values[row]) & (pd.DatetimeIndex(ds_u.time).day == track['day'].values[row]), latitude=(ds_u.latitude >= bbox_wind[1]) & (ds_u.latitude <= bbox_wind[3]), longitude=(ds_u.longitude >= bbox_wind[0]) & (ds_u.longitude <= bbox_wind[2]))
    v_bbox = ds_v.sel(time=(pd.DatetimeIndex(ds_v.time).hour == track['hour'].values[row]) & (pd.DatetimeIndex(ds_v.time).day == track['day'].values[row]), latitude=(ds_v.latitude >= bbox_wind[1]) & (ds_v.latitude <= bbox_wind[3]), longitude=(ds_v.longitude >= bbox_wind[0]) & (ds_v.longitude <= bbox_wind[2]))
    
    wind_bbox = xr.merge([u_bbox, v_bbox])
    wind_bbox.to_netcdf(track['wind_file_era5'].values[row])
    return None

# %%
def create_track_list_with_wind_files(tracks, track_id):
    track_nth = tracks[tracks.track_id_local == track_id]
    lon = track_nth['lon'].values
    lat = track_nth['lat'].values
    track_bbox = track_nth[extend_track((lon >= bbox[0]) & (lon <= bbox[2]) & (lat >= bbox[1]) & (lat <= bbox[3]))].drop(['i', 'j', 'slp'], axis=1)
    track_bbox = insert_missing_time(track_bbox)
    track_bbox['wind_file_era5'] = track_bbox.apply(lambda x: 'winds_1hrly_era5_era5track/boston/' + str(track_id).zfill(4) + '_' + str(int(x['year'])) + str(int(x['month'])).zfill(2) + str(int(x['day'])).zfill(2) + str(int(x['hour'])).zfill(2) + '.nc', axis=1)
    if extract:
        for row in range(len(track_bbox)):
            extract_era5_wind(track_bbox, row)
    return track_bbox

# %%
filtered_tracks = Parallel(n_jobs=6)(delayed(create_track_list_with_wind_files)(tracks, track_id) for track_id in tracks.track_id_local.unique())
filtered_tracks_merged = pd.concat(filtered_tracks)

# %%
filtered_tracks_merged.to_csv('tracks_1hrly/ERA5_XTC_Boston_winds.csv', index=False)


