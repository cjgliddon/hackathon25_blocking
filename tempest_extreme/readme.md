# hackathon25_blocking
'''
Tempest Extreme Code
The following procedure uses tempest extreme algo to get all tracks that passes throught Boston

    module load apps/miniconda/3.6
    Source activate tempest_extreme
        
    
    1. Detect_nodes_parallel.sh (creates nodes_1hrly folder )
        a. chmod +x detect_nodes_parallel.sh
        b. ./detect_nodes_parallel.sh
        
    2. Stitch_nodes_1hrly_era5.sh (creates tracks_1hrly folder, csv all)
        a. chmod +x stitch_nodes_1hrly_era5.sh
        b. ./stitch_nodes_1hrly_era5.sh
        
    3. python filter_tracks.py (csv for boston only)
    
python extract_era5_winds.py (winds_1hrly_era5_era5track/boston/)
'''
