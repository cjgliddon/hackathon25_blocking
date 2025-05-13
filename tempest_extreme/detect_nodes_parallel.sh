#!/bin/bash
# This script is used to extract low-pressure nodes from ERA5 data in parallel, at hourly interval.
# This is a prototype for running the tracking algorithm parallelly, without submitting job to the queue.
# Even though the script is written for Cheyenne/GLADE, Do not run it at Cheyenne login node! Run submit_detect_nodes_jobs.sh instead.
# If running at nusydo/flood/momo, change the path to ERA5 data accordingly. You'd also need to change file names. And the executable path.
# For explanation on Parallelization, see: https://unix.stackexchange.com/a/216475

# years=`seq 2022 -1 1941`
# years=`seq 2022 -1 2010`
# years=`seq 2009 -1 1997`
# years=`seq 1996 -1 1984`
# years=`seq 1983 -1 1971`
# years=`seq 1970 -1 1958`
years=`seq 1957 -1 1945`
months=('01' '02' '03' '04' '05' '06' '07' '08' '09' '10' '11' '12')

ERA5_folder=/net/flood/data/projects/liberty/data/era5 #/glade/collections/rda/data/ds633.0
mkdir -p nodes_1hrly

# initialize a semaphore with a given number of tokens
open_sem(){
    mkfifo pipe-$$
    exec 3<>pipe-$$
    rm pipe-$$
    local i=$1
    for((;i>0;i--)); do
        printf %s 000 >&3
    done
}

# run the given command asynchronously and pop/push tokens
run_with_lock(){
    local x
    # this read waits until there is something to read
    read -u 3 -n 3 x && ((0==x)) || exit $x
    (
     ( "$@"; )
    # push the return code of the command to the semaphore
    printf '%.3d' $? >&3
    )&
}

# Extract Nodes from ERA5 Data
extract_nodes(){
    local yr=$1
    local mth=$2

    # Prepare list of input files
    [ ! -e files_$yr$mth.tmp ] || rm files_$yr$mth.tmp
    # msl_file=$ERA5_folder/e5.oper.an.sfc/${yr}${mth}/e5.oper.an.sfc.128_151_msl.ll025sc.${yr}${mth}0100_${yr}${mth}$(cal $mth $yr | awk 'NF {DAYS = $NF}; END {print DAYS}')23.nc
    msl_file=$ERA5_folder/era5_msl_${yr}_${mth}.nc
    echo -e "${msl_file}" >> files_$yr$mth.tmp

    # Output file
    node_file=nodes_1hrly/${yr}${mth}

    # Detect Nodes
    /home/x_yan/.conda/envs/tempest_extreme/bin/DetectNodes \
        --in_data_list files_$yr$mth.tmp \
        --out $node_file \
        --searchbymin msl \
        --closedcontourcmd "msl,200.0,6.0,0" \
        --mergedist 6.0 \
        --outputcmd  "msl,min,0" \
        --latname latitude --lonname longitude
    
    # Cleanup
    rm files_$yr$mth.tmp
}

# initialize semaphore
open_sem 12
# run 12 jobs in parallel
for yr in ${years[@]}; do
    for mth in ${months[@]}; do
        run_with_lock extract_nodes $yr $mth &
    done
done
