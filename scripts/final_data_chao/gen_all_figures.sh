#!/bin/bash

echo "gen eval_overall.pdf..."
python figure1_chao.py finaldata/edr/ finaldata/fdr/
echo "gen qps_scalability.pdf..."
python ../final_data_for_isca_2020/rebuttal/qps_scalability.py finaldata/qp_out/
echo "gen latency-tput.pdf..."
python tput-lat-bank-ycsb.py .
echo "gen eval_conflict.pdf..."
python figure2_conf.py finaldata/conf_curve_ycsb_28/
echo "gen eval_exe_workload.pdf..."
python figure3_sleep.py finaldata/sleep_curve/
echo "gen eval_coroutines.pdf..."
python figure4.py finaldata/increasing_cor_num_bank/ finaldata/increasing_cor_num/routine_28_with_calvin/
echo "gen lat_breakdown.pdf..."
runipy figure_lat_breakdown.ipynb
echo "gen network_trips.pdf..."
runipy figure_round_trips.ipynb
