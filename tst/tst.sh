#!/bin/bash

scriptnm=`echo $0 | sed 's@.*/\(.*\)@\1@'`

mxn="python3 mxn.py"
init=0
#as_host=dave
as_host=127.0.0.1
as_port=3000
mxn_rec_cnt=1000
process_cnt=10
txn_cnt=100
fail_pct=0  # 10ths of a percent

usage () {
  echo "$scriptnm: -i -h -p -rcnt -pcnt -tcnt -fpct"
  echo "where:"
  echo "  -i        initialize data set [$init]"
  echo "  -h        aerospike host [$as_host]"
  echo "  -p        aersopike port [$as_port]"
  echo "  -rcnt     record count [$mxn_rec_cnt]"
  echo "  -pcnt     process count [$process_cnt]"
  echo "  -tcnt     transaction count [$txn_cnt]"
  echo "  -fpct     failure percent in 10ths from 0-100 [$fail_pct]"
  exit 1
}

[ $# -eq 0 ] && usage
while [ $# -gt 0 ]; do

  case $1 in

    # initialize data set
    -i) init=1 ;;

    # asd host
    -h) shift; as_host=$1 ;;

    # asd port
    -p) shift; as_port=$1 ;;

    # record count
    -rcnt) shift; mxn_rec_cnt=$1 ;;

    # process count
    -pcnt) shift; process_cnt=$1 ;;

    # transaction count
    -tcnt) shift; txn_cnt=$1 ;;

    # failure percentage
    -fpct) shift; [ $1 -lt 0 -o $1 -gt 100 ] && usage
           fail_pct=$1 ;;

    # assume program name specification
    *) usage ;;

  esac
  shift

done


echo
echo "Starting test with:"
echo "  init:           ${init}"
echo "  as_host:        ${as_host}"
echo "  as_port:        ${as_port}"
echo "  mxn_rec_cnt:    ${mxn_rec_cnt}"
echo "  process_cnt:    ${process_cnt}"
echo "  txn_cnt:        ${txn_cnt}"
echo "  fail_pct:       ${fail_pct}"
echo


# truncate and reinitialize data set
# all 3 must be in, or out, together
if [ ${init} -eq 1 ]; then

  # truncate mxn set
  echo mxn -host ${as_host} -port ${as_port} -c trunc
  ${mxn} -host ${as_host} -port ${as_port} -c trunc

  # truncate mxnlog set
  echo mxn -host ${as_host} -port ${as_port} -c trunc -s mxnlog
  ${mxn} -host ${as_host} -port ${as_port} -c trunc -s mxnlog

  # initialize data set
  echo mxn -host ${as_host} -port ${as_port} -c mputs -n ${mxn_rec_cnt} -u usr
  ${mxn} -host ${as_host} -port ${as_port} -c mputs -n ${mxn_rec_cnt} -u usr

fi


# start background processes to drive transactions
for i in $(seq 1 ${process_cnt}); do
  ${mxn} -host ${as_host} -port ${as_port} -c rtxns -n ${txn_cnt} -fpct ${fail_pct} -dlvl dbg -dtrc -dlog x.mxndbg${i} > x.rtxns${i} &
  echo [$!] mxn -host ${as_host} -port ${as_port} -c rtxns -n ${txn_cnt} -fpct ${fail_pct} -dlvl dbg -dtrc -dlog x.mxndbg${i} \> x.rtxns${i}
  pids[${i}]=$!
done

# wait on procs
echo waiting for procs to complete..
for pid in ${pids[*]}; do
  wait ${pid}
  echo pid:${pid} done
done

exit 0


echo waiting for locks to expire - sleep 10
sleep 10

# check reconciliation of accounts
echo mxn -host ${as_host} -port ${as_port} -c rcn
${mxn} -host ${as_host} -port ${as_port} -c rcn

echo mxn -host ${as_host} -port ${as_port} -c scan \> x.mxn
${mxn} -host ${as_host} -port ${as_port} -c scan > x.mxn-scan

echo mxn -host ${as_host} -port ${as_port} -c scan -s mxnlog \> x.mxnlog
${mxn} -host ${as_host} -port ${as_port} -c scan -s mxnlog > x.mxnlog-scan

echo mxn -host ${as_host} -port ${as_port} -c cln -dlvl dbg -dtrc \> x.cln
${mxn} -host ${as_host} -port ${as_port} -c cln -dlvl dbg -dtrc -dlog x.clndbg > x.cln

echo mxn -host ${as_host} -port ${as_port} -c scan \> x.mxn2
${mxn} -host ${as_host} -port ${as_port} -c scan > x.mxn-scan2

echo mxn -host ${as_host} -port ${as_port} -c scan -s mxnlog \> x.mxnlog2
${mxn} -host ${as_host} -port ${as_port} -c scan -s mxnlog > x.mxnlog-scan2

echo mxn -host ${as_host} -port ${as_port} -c rcn
${mxn} -host ${as_host} -port ${as_port} -c rcn
