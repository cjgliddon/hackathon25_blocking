# %%
import pandas as pd
from joblib import Parallel, delayed
import os

# %%
boston_bbox = [288, 41.25, 290, 43.25] # west, south, east, north of ~100Km (1 degree) distance from Boston

# %%
tracks_global = pd.read_csv('tracks_1hrly/ERA5_TempestExtremes_XTC.csv')

# %%
def check_if_track_within_bbox(global_tracks, track_id, bbox):
    lon = global_tracks[global_tracks.track_id == track_id]['lon'].values
    lat = global_tracks[global_tracks.track_id == track_id]['lat'].values
    if ((lon >= bbox[0]) & (lon <= bbox[2]) & (lat >= bbox[1]) & (lat <= bbox[3])).any():
        return track_id

# %%
track_list = Parallel(n_jobs=os.cpu_count()-10)(delayed(check_if_track_within_bbox)(tracks_global, track_id, boston_bbox) for track_id in range(0, tracks_global.track_id.max()+1))
boston_etc_track_ids = [i for i in track_list if i is not None]

# %%
tracks_boston = tracks_global[tracks_global.track_id.isin(boston_etc_track_ids)].reset_index(drop=True)
tracks_boston.rename(columns={"track_id": "track_id_global"}, inplace=True)
tracks_boston['track_id_local'] = tracks_boston.track_id_global.rank(method='dense').astype(int)-1
tracks_boston = tracks_boston[tracks_boston.columns.tolist()[-1:] + tracks_boston.columns.tolist()[:-1]] #reorder columns

# %%
tracks_boston.to_csv('tracks_1hrly/ERA5_TempestExtremes_XTC_Boston.csv', index=False)


