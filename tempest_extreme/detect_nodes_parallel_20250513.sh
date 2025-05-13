#!/usr/bin/env bash
# detect_nodes_xshield.sh
# Detect low-height nodes at 1000 hPa in X-SHiELD output with TempestExtremes.

set -euo pipefail
shopt -s nullglob

###############################################################################
# USER SETTINGS — edit to suit your environment
###############################################################################
if command -v module &>/dev/null; then                 # HPC/Lmod
    module -t avail nco  &>/dev/null && module load nco
fi
command -v ncks >/dev/null 2>&1 || { echo "ncks is required – aborting"; exit 2; }

BASE_DIR=/scratch/cimes/GLOBALFV3/20191020.00Z.C3072.xs24v2/history
OUT_DIR=/scratch/cimes/xy4043/tracks
TE_EXE=/home/xy4043/.conda/envs/tempest_extreme/bin/DetectNodes
NPROC=12
TMP_DIR=${SLURM_TMPDIR:-$PWD}
###############################################################################

mkdir -p "$OUT_DIR"/nodes_1hrly

###############################################################################
# PARALLEL SEMAPHORE
###############################################################################
open_sem() { mkfifo "pipe-$$"; exec 3<>"pipe-$$"; rm -f "pipe-$$";
             for ((i=0;i<$1;i++)); do printf 000 >&3; done; }
run_with_lock() { local x; read -u 3 -n 3 x && ((x==0)) || exit "$x";
                  ( "$@"; printf '%.3d' $? >&3 ) & }

###############################################################################
# CORE WORKER
###############################################################################
extract_nodes() {
    local init="$1"
    local src="$BASE_DIR/$init/h_plev_coarse_C3072_1440x720.fre.nc"
    [[ -f $src ]] || { echo "Missing $src" >&2; return 1; }

    # ------------------ slice to 1000 hPa and fix calendar -------------------
    local use_nc lvl_nc
    lvl_nc="$TMP_DIR/${init}_h1000.nc"
    ncks -O -d plev,1000 "$src" "$lvl_nc"

    # If file says calendar="360_day", change to "standard" so TE won't crash.
    # cdo is fastest; fall back to ncatted if cdo is unavailable.
    if command -v cdo >/dev/null 2>&1; then
        cdo -O setcalendar,standard "$lvl_nc" "${lvl_nc%.nc}_fix.nc" \
            && mv "${lvl_nc%.nc}_fix.nc" "$lvl_nc"
    else
        ncatted -O -a calendar,time,o,c,"standard" "$lvl_nc"
    fi
    use_nc="$lvl_nc"                                      # always use the slice

    # ------------------ DetectNodes ---------------------
    local list="$TMP_DIR/files_${init}.lst";  echo "$use_nc" > "$list"
    local out_stub="$OUT_DIR/nodes_1hrly/${init}"

    "$TE_EXE" \
        --in_data_list     "$list" \
        --out              "$out_stub" \
        --searchbymin      h_plev_coarse \
        --closedcontourcmd "h_plev_coarse,50.0,6.0,0" \
        --mergedist        6.0 \
        --outputcmd        "h_plev_coarse,min,0" \
        --latname          grid_yt_coarse \
        --lonname          grid_xt_coarse

    rm -f "$list" "$lvl_nc"
    echo "✓  Finished $init"
}

###############################################################################
# MAIN LOOP
###############################################################################
open_sem "$NPROC"
for dir in "$BASE_DIR"/20??????00; do
    run_with_lock extract_nodes "$(basename "$dir")"
done
wait
echo "All DetectNodes jobs finished."
