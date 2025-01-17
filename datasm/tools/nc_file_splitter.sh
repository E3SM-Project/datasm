#!/bin/bash

infile=$1

inpath=`dirname $infile`
infile=`basename $infile`

fsiz=`du -b $inpath/$infile | cut -f1`

file_id=`echo $infile | rev | cut -f2- -d_ | rev`

date_part=`echo $infile | rev | cut -f1 -d_ | rev | cut -f1 -d.`
start_date=`echo $date_part | cut -f1 -d-`
final_date=`echo $date_part | cut -f2 -d-`
start_year=`echo $start_date | cut -c1-4`
final_year=`echo $final_date | cut -c1-4`

range_years=$((10#$final_year - 10#$start_year + 1))

echo "FILE_SZ: $fsiz"
echo "FILE_ID: $file_id"
echo "YEARS: $start_year $final_year ($range_years years)"

size_threshold=5000000000       # desired max file size 5GB
file_size_per_year=$((fsiz/range_years))
for ypf in 500 200 100 50 20 10 5 1; do
    try_seg_size=$((file_size_per_year*ypf))
    if [[ $try_seg_size -gt $size_threshold ]]; then
        continue
    fi
    break
done

echo "YPF = $ypf"
range_segs=$((range_years/ypf))
echo "full segments = $range_segs"

partial_seg=0
if [[ $((range_segs*ypf)) -lt $range_years ]]; then
    partial_seg=1
    partial_years=$((range_years - range_segs*ypf))
fi

if [[ $partial_seg -eq 1 ]]; then
    echo "Last segment = $partial_years years"
fi
    

# adapted from code provided by Charlie Zender

range_start=$start_year
mon_per_seg=$((12*ypf))

for ((segdex=0;segdex<range_segs;segdex++)); do
    # indexing is by month-count
    srt_idx=$((segdex*mon_per_seg))
    end_idx=$((srt_idx + mon_per_seg - 1))
    beg_year=$((10#$start_year + segdex*ypf))
    beg_date=`printf "%04d" "$beg_year"`01
    end_year=$((10#$start_year + (segdex + 1)*ypf - 1))
    end_date=`printf "%04d" "$end_year"`12
    echo "cmd = ncrcat -O -d time,${srt_idx},${end_idx} $inpath/$infile ${file_id}_${beg_date}-${end_date}.nc"
done

if [[ $partial_seg == 1 ]]; then
    srt_idx=$((segdex*mon_per_seg))
    mon_this_seg=$((partial_years*12))
    end_idx=$((srt_idx + mon_this_seg - 1))

    beg_year=$((10#$start_year + segdex*ypf))
    beg_date=`printf "%04d" "$beg_year"`01
    end_year=$((10#$start_year + segdex*ypf + partial_years - 1))
    end_date=`printf "%04d" "$end_year"`12
    echo "cmd = ncrcat -O -d time,${srt_idx},${end_idx} $inpath/$infile ${file_id}_${beg_date}-${end_date}.nc"

fi

exit 0


WE WANT OUTPUT LIKE:

uo_Omon_E3SM-2-1_historical_r1i1p1f1_gr_185001-185412.nc
uo_Omon_E3SM-2-1_historical_r1i1p1f1_gr_185501-185912.nc
uo_Omon_E3SM-2-1_historical_r1i1p1f1_gr_186001-186412.nc
uo_Omon_E3SM-2-1_historical_r1i1p1f1_gr_186501-186912.nc
uo_Omon_E3SM-2-1_historical_r1i1p1f1_gr_187001-187412.nc
. . .

or

zhalfo_Omon_E3SM-2-1_piControl_r1i1p1f1_gr_000101-000512.nc
zhalfo_Omon_E3SM-2-1_piControl_r1i1p1f1_gr_000601-001012.nc
zhalfo_Omon_E3SM-2-1_piControl_r1i1p1f1_gr_001101-001512.nc
zhalfo_Omon_E3SM-2-1_piControl_r1i1p1f1_gr_001601-002012.nc
zhalfo_Omon_E3SM-2-1_piControl_r1i1p1f1_gr_002101-002512.nc


General format of CMIP6 output file name:

    <file-id>_<first-year-month>-<last-year-month>.nc

where <file-id> = <cmip6var>_<table>_<source_id>_<experiment>_<variant>_<grid>

    e.g.        so_Omon_E3SM-2-1_1pctCO2_r1i1p1f1_gr

and first and last year and month each have the format YYYYMM, thus

                so_Omon_E3SM-2-1_1pctCO2_r1i1p1f1_gr_185001-185912

or              so_Omon_E3SM-2-1_piControl_r1i1p1f1_gr_000101-000912




dcd_srt='1850' # Starting decade
for dcd_idx in {0..16}; do # Assume 16 decades in input file
   dcd=`printf "%d" $dcd_idx`
   let srt_idx=${dcd}*120 # 120 months per decade
   let end_idx=${srt_idx}+119
   ncrcat -O -d time,${srt_idx},${end_idx} in.nc out_${srt_idx}s.nc done

