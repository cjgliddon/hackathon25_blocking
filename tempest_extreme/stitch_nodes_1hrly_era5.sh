#!/bin/bash
# ls nodes_1hrly_era5/* > nodeslist.tmp
ls nodes_1hrly/* > nodeslist.tmp
mkdir -p tracks_1hrly

/home/x_yan/.conda/envs/tempest_extreme/bin/StitchNodes \
        --in_list nodeslist.tmp \
        --out tracks_1hrly/ERA5_TempestExtremes_XTC.csv \
        --in_fmt "lon,lat,slp" \
        --range 2.0 \
        --mintime "60h" \
        --maxgap "8h" \
        --min_endpoint_dist 12.0 \
        --out_file_format "csv"

rm nodeslist.tmp

# Remove spaces from column names
# python << END
# Process the output (only if stitching succeeded)
if [ -f "tracks_1hrly/ERA5_TempestExtremes_XTC.csv" ]; then
    python << END

import pandas as pd
track = pd.read_csv('tracks_1hrly/ERA5_TempestExtremes_XTC.csv')
track.columns = track.columns.str.replace(' ', '')
track.to_csv('tracks_1hrly/ERA5_TempestExtremes_XTC.csv', index=False)
END
else
    echo "Error: Stitching failed - output file not created"
fi