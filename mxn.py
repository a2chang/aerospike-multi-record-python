#!/usr/bin/env python3

from __future__ import print_function
import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import operations
from aerospike_helpers.operations import map_operations
from aerospike import predicates as p

from aslib.dbg import Dbg
from aslib.asutils import Lck
from aslib.asutils import ClientWrapper

import inspect
import json
import os
import random
import signal
import sys
import time
import uuid


# TODO:
#
# add check for 2nd idx before calling query; or just once in run before loop
#
# add support to create 2nd index
#

cmd_info = [
  "help:    Print long help on Lck and this script\n",
  "put:     Put a record\n",
  "mputs:   Multi-put calls\n",
  "get:     Get a record\n",
  "op:      Run operate command\n",
  "del:     Delete a record\n",
  "lck:     Lock a record\n",
  "uck:     Unlock a record\n",
  "clr:     Clear a stale lock\n",
  "trunc:   Truncate a set\n",
  "scan:    Scan a set\n",
  "txn:     Run a transaction\n",
  "rtxns:   Random txn calls\n",
  "rcn:     Reconcile logs to accounts\n",
  "cln:     Cleanup stale pending transactions\n",
  "clnsvc:  Cleanup service; loops 4eva calling cleanup\n",
  "info:    Request info; ukey is info 'command'\n",
    # https://www.aerospike.com/docs/reference/info/
  "tst:     Testing hook\n"
]
cmds = []
for cmd in cmd_info:
  cmds.append(cmd.split(':')[0])
cmd_hlp = ""
for cmd in cmd_info:
  cmd_hlp += cmd

try:
  as_host = os.environ['AS_HOST']
except:
  as_host = '127.0.0.1'           # asdb host
try:
  as_port = os.environ['AS_PORT']
except:
  as_port = 3000                  # asdb port

as_ns = 'test'                  # asdb namespace
as_set = 'mxn'                  # asdb master set

fpct = 0                        # Failure percentage in 10ths of a percent

# rec: { name: nm, bal: bal,
#        lck: { txnid: tid, ukey: ky},
#        lckto: to, ptxns: [tid, tid2] } 

now = lambda: int(round(time.time() * 1000))


# command line parser
def main_parser (argv):

  import argparse

  # define command line parser
  parser = argparse.ArgumentParser (
    description="Multi-transaction test tool.")

  # cmd; see cmd_info above
  parser.add_argument (
    "-cmd", metavar="cmd",
    #required=True,
    choices=cmds,
    help="PPrint:Command to execute.\n""{}".format(cmd_hlp)
  )

  # namespace
  parser.add_argument (
    "-ns", "--namespace", metavar="namespace", dest="ns",
    #required=True,
    default=as_ns,
    help="Aerospike namespace["+as_ns+"].")

  # set
  parser.add_argument (
    "-set", metavar="set", dest="set",
    #required=True,
    default=as_set,
    help="Aerospike set["+as_set+"].")

  # user key
  parser.add_argument (
    "-ukey", "--userKey", metavar="user_key", dest="ukey",
    help="Aerospike user key.")

  # transaction Id
  parser.add_argument (
    "-txnid", "--transactionId", metavar="txnid", dest="txnid",
    help="Transaction Id.")

  # record
  parser.add_argument (
    "-rec", "--record", metavar="record", dest="rec",
    help="PPrint:Aerospike record as a JSON string.\n"
    "e.g. '{\"name\": \"john\", \"bal\": 1000}'")

  # operations
  parser.add_argument (
    "-ops", "--operations", metavar="ops", dest="ops",
    help="PPrint:operations to execute; e.g.\n"
    "'[ operations.read (\"name\"),\n"
    "  operations.read (\"bal\")]'")

  # transaction amount
  parser.add_argument (
    "-amt", "--amount", metavar="amount", dest="amt",
    type=int,
    help="Amount of transaction.")

  # count
  parser.add_argument (
    "-n", "--count", metavar="count", dest="cnt",
    type=int,
    help="Count used with 'rtxns' or 'mputs' commands.")

  # failure percentage
  parser.add_argument (
    "-fpct", "--failure_percent", metavar="percent", dest="fpct",
    type=int,
    default=fpct,
    help="Failure percentage in 10ths of a percent (10 is 1%%; 1 is 0.1%%).  Introduces simulated asdb failures at given percentage.")

  # no exists
  parser.add_argument (
    "-nx", "--no_exists", action='store_true', dest="nx",
    help="Record must not exist. Policy flag used with 'put' command.")

  # gen match
  parser.add_argument (
    "-gen", "--generation", metavar="generation", dest="gen",
    type=int,
    default=0,
    help="Record generation match value. Used with 'put' command.")

  # host
  parser.add_argument (
    "-host", metavar="host",
    default=as_host,
    help="Aerospike seed host name ["+as_host+"].")

  # port
  parser.add_argument (
    "-port", metavar="port",
    type=int,
    default=as_port,
    help="Aerospike seed host port["+str(as_port)+"].")

  # add dbg parser for debug switches
  dbg_parser = Dbg.arg_parser(parser)

  # hack to show usage w/o the myriad dbg switches; get those w/ '-h'
  #   main_parser(['usage', emsgs])
  if argv[0] == 'usage':
    for emsg in argv[1]:
      print ("Error: {}".format(emsg))
    print ("\n{}".format(parser.format_usage()))
    return

  return dbg_parser.parse_args(argv[1:])
  #return parser.parse_args(argv[1:])

  # main_parser


# Create a client and connect it to the cluster
def aero_connect (cfg, d=None):

  if d is None: d = Dbg('none')
  d.enter ("cfg:{}".format(cfg))

  # check args
  errs = ''
  if cfg is None: errs += "cfg is req'd; "
  if errs: d.throw (ValueError, errs)

  try:
    client = aerospike.client(cfg).connect()

  except Exception as x:
    xmsg = "Failed to connect to the cluster with {}".format(cfg['hosts'])
    msg2 = "N.B. AS_HOST and AS_PORT env vars reset defaults of 127.0.0.1:3000"
    d.err (xmsg+"\n"+msg2)
    d.exc (x, xmsg, lvl=d.DBG)
    d.leave ()
    return None

  if fpct is not None and fpct > 0:
    client = ClientWrapper (client, fpct=fpct)

  d.leave ()
  return client

  # aero_connect


# Get list of namespaces
def info_hndlr (client, cmd, d=None):

  if d is None: d = Dbg('none')
  d.enter ()

  errs = ''
  if cmd is None: errs += "cmd is req'd; "
  if errs: d.throw (ValueError, errs)

  res = client.info_all (cmd)

  d.leave (res)
  return res


# Write a record
def put_hndlr (client, ns, set, ukey, recstr, nx=False, gen=0, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{} recstr:{} nx:{}"\
         .format(ns, set, ukey, recstr, nx))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if recstr is None: errs += "recstr is req'd; "
  if errs: d.throw (ValueError, errs)

  key = (ns, set, ukey)
  rec = json.loads(recstr)

  policy = {}
  meta = {}
  if nx:
    policy['exists'] = aerospike.POLICY_EXISTS_CREATE
  if gen > 0:
    policy['gen'] = aerospike.POLICY_GEN_EQ
    meta['gen'] = gen

  try:
    for ky in rec:
      # dmf: hack to provide for bin removal
      if rec[ky] == 'None':
        rec[ky] = aerospike.null()
    client.put(key, rec, meta=meta, policy=policy)

  except Exception as x:
    xmsg = "Exception: {}".format(x)
    d.exc (x, xmsg, lvl=d.ERR)
    d.leave (-1)
    return -1

  d.leave (0)
  return 0

  # put_hndlr


# Multi-put handler
#   ukey is used as a prefix, appended with count
def mputs_hndlr (client, ns, set, ukey, cnt, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{} cnt:{}".format(ns, set, ukey, cnt))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if cnt is None: errs += "cnt is req'd; "
  if errs: d.throw (ValueError, errs)

  rc = 0
  for cnt in range(cnt):
    rkey = ukey+str(cnt)
    rec = {"name": rkey, "bal": 0}
    recstr = json.dumps(rec,sort_keys=True)
    rc = put_hndlr (client, ns, set, rkey, recstr, nx=False, gen=0, d=d)
    if rc < 0:
      d.err ("Failed to put {}.{}.{} {}".format(ns,set,rkey,recstr))
      d.leave (rc)
      return rc

  d.leave (rc)
  return rc

  # mputs_hndlr


# Read a record
def get_hndlr (client, ns, set, ukey, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{}".format(ns, set, ukey))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if errs: d.throw (ValueError, errs)

  key = (ns, set, ukey)

  try:
    (rkey, meta, rec) = client.get(key)

  except Exception as x:
    xmsg = "Exception: {}".format(x)
    d.exc (x, xmsg)
    d.leave ()
    return (None, None, None)

  # d.leave ((meta, json.dumps(rec,sort_keys=True)))
  d.leave ((meta, rec))
  return (rkey, meta, rec)

  # get_hndlr


# Read a record
def operate_hndlr (client, ns, set, ukey, ops, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{}".format(ns, set, ukey))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if errs: d.throw (ValueError, errs)

  key = (ns, set, ukey)

  try:
    meta = None
    policy = None
    (rkey, meta, rec) = client.operate(key, eval(ops), meta, policy)

  except Exception as x:
    xmsg = "Exception: {}".format(x)
    d.exc (x, xmsg, lvl=d.ERR)
    d.leave ()
    return (None, None, None)

  d.leave ((meta, json.dumps(rec,sort_keys=True)))
  return (rkey, meta, rec)

  # operate_hndlr


# Delete a record
def del_hndlr (client, ns, set, ukey, no_exist_allowed=False, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{}".format(ns, set, ukey))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if errs: d.throw (ValueError, errs)

  key = (ns, set, ukey)

  try:
    client.remove(key)

  except Exception as x:
    if no_exist_allowed and type(x) == ex.RecordNotFound:
      d.dbg ("record did not exist, but that's ok")
      d.leave (0)
      return 0
    else:
      xmsg = "Exception: {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)
      d.leave (-1)
      return -1

  d.leave (0)
  return 0

  # del_hndlr


# Truncate a set
def trunc_hndlr (client, ns, set, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{}".format(ns, set))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if errs: d.throw (ValueError, errs)

  rc = 0
  try:
    rc = client.truncate (ns, set, 0)

  except Exception as x:
    xmsg = "Exception: {}".format(x)
    d.exc (x, xmsg)
    d.leave (-1)
    return -1

  d.leave (rc)
  return rc

  # trunc_hndlr


# Scan a set
def scan_hndlr (client, ns, set, callback, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} callback:{}".format(ns, set, callback))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if callback is None: errs += "callback is req'd; "
  if errs: d.throw (ValueError, errs)

  try:
    if set == '':
      res = client.scan(ns,None)
    else:
      res = client.scan(ns, set)
    res.foreach(callback)
    d.leave (0)
    return 0

  except Exception as x:
    xmsg = "Exception: {}".format(x)
    d.exc (x, xmsg)
    d.leave ()
    return

  d.leave ()
  return

  # scan_hndlr


# Transaction handler
#   lock record
#   log transaction
#   settle transaction, and unlock record
def txn_hndlr (client, ns, set, ukey, txnid, amt, usr_ops=None, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{} amt:{} usr_ops:{}"\
           .format(ns, set, ukey, amt, usr_ops))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if txnid is None: errs += "txnid is req'd; "
  if amt is None: errs += "amt is req'd; "
  if errs: d.throw (ValueError, errs)

  # lock the record
  try:
    lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
    (rkey, rmeta, rec) = lck.acquire ()
  except Exception as x:
    xmsg = "Failed to acquire lock; {}".format(x)
    d.exc (x, xmsg, lvl=d.ERR)
    d.leave (xmsg)
    return (None, None, None)

  d.dbg ("rec:{} locked".format(ukey))

  # log info
  log_set = as_set + 'log'
  log_key = ukey + ":" + txnid
  log_bin = 'log'
  log_rec = {"txnid": txnid, "amt": str(amt), "set": set, "ukey": ukey}
  log_rec_str = json.dumps(log_rec,sort_keys=True)

  # log transaction
  rc = 0
  try:
    rc = put_hndlr (client, ns, log_set, log_key, log_rec_str, nx=True, d=d)
  except:
    d.err ("Error: failed putting rec:{} to set:{} in 'txn'"\
           .format(log_rec, log_set))

    d.leave ((None, None, None))
    return (None, None, None)

  if rc < 0:
    d.dbg ("Failed to put {}.{}.{} {}".format(ns,log_set,log_key,log_rec_str))
    d.leave ((None, None, None))
    return (None, None, None)

  d.dbg ("logged txn {}".format(log_rec_str))

  settle_ops = [
    operations.increment("bal", amt),
    operations.read('name'),
    operations.read('bal')
  ]

  # add user ops
  if usr_ops is not None:
    for op in usr_ops:
      d.dbg ("usr op:{}".format(op))
      settle_ops.append (op)

  # settle txn and release the lock
  try:
    (rkey, rmeta, rec) = lck.release (settle_ops)
  except Exception as x:
    d.err ("Exception caught releasing lock; {}".format(x))
    d.leave ((None, None, None))
    return (None, None, None)

  d.dbg ("rec {} settled: {}".format(ukey,rec))

  d.leave ((rmeta, json.dumps(rec,sort_keys=True)))
  return (rkey, rmeta, rec)

  # txn_hndlr


# Random transaction handler
#   Apply cnt transactions to random account records
def rtxns_hndlr (client, ns, set, ukey, cnt, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} cnt:{}".format(ns, set, cnt))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if cnt is None: errs += "cnt is req'd; "
  if errs: d.throw (ValueError, errs)

  # Transact on a specific ukey
  if ukey is not None:
    (rkey, meta, rec) = get_hndlr (client, ns, set, ukey, d=d)
    if rec is None:
      d.err ("Failed to get ({}:{}:{})".format(ns,set,ukey))
      d.leave (-1)
      return -1
    usr_keys = [] # user keys
    usr_amts = {}  # user amounts
    usr_keys.append(rec['name'])
    usr_amts[rec['name']] = rec['bal']

  # Get list of ns.set usr_keys in order to randomly transact on them
  else:
    usr_keys = []
    usr_amts = {}
    def get_names (kmr):
      try:
        rec = kmr[2]
        usr_keys.append(rec['name'])
        usr_amts[rec['name']] = rec['bal']
      except Exception as x:
        xmsg = "rtxns: get_names: rec:{} Exception: {}".format(rec, x)
        d.exc (x, xmsg, lvl=d.ERR)

    scan_hndlr (client, ns, set, get_names, d=d)
  
  # Need records to transact upon
  if len(usr_keys) == 0:
    d.err ("{}.{} set is empty! Initial records required".format(ns, set))
    d.leave (-1)
    return -1
    
  d.dbg ("Initial balances: {}".format(usr_amts))

  ops = [ operations.read('name'), operations.read('bal') ]

  # Loop executing random transactions
  for cnt in range(cnt):

    ukey = usr_keys[random.randint(0,len(usr_keys)-1)]
    amt = random.randint(-100, 1000)
    txnid = str(uuid.uuid4())

    (rkey, meta, rec) = txn_hndlr (client, ns, set, ukey, txnid, amt, ops, d=d)
    if rec is None:
      d.err ("Error: Failed transaction; not aggregating ukey:{} amt:{}"\
             .format(ukey,amt))
    else:
      usr_amts[ukey] += amt
      if d.dlvl >= d.INF:
        d.out ("ran txn:{} amt:{:6d} rec:{}"\
               .format(txnid,amt,json.dumps(rec,sort_keys=True)))
        d.out ("{}".format(usr_amts))
        scan_hndlr (client, ns, set,
                    lambda kmr: d.out ("{}".format(json.dumps(kmr[2],sort_keys=True))) , d=d)

  d.leave (0)
  return 0

  # rtxns_hndlr


# Reconciliation handler
#   aggregate txn logs and compare to account bals
def rcn_hndlr (client, ns, set, ukey=None, d=None):

  if d is None: d = Dbg('none')
  d.enter ()

  # loop over recs
  def get_recs (kmr):

    d.dbg ("get_recs entered")

    scan_rec = kmr[2]
    lck_rec = {}
    ukey = scan_rec['name']
    txnid = str(uuid.uuid4())

    # lock the record
    try:
      usr_ops = [ operations.read ('bal') ]
      lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
      (rkey, rmeta, lck_rec) = lck.acquire (usr_ops)
    except Exception as x:
      lck_fails.append(ukey)
      xmsg = "Failed to acquire lock; {}".format(x)
      # d.inf (xmsg)
      d.exc (x, xmsg, lvl=d.ERR)
      #d.leave (xmsg)
      return

    d.dbg ("lck'd ukey:{} scan_rec:{} lck_rec:{}".format(ukey,scan_rec,lck_rec))

    rec_bal = int(lck_rec['bal'])
    pend_amt = 0
    log_amt = 0

    d.dbg ("lck_rec:{}".format(lck_rec))
    d.dbg ("rec_bal:{} pend_amt:{} log_amt:{}".format(rec_bal, pend_amt, log_amt))

    rec_ptxn_ukeys = []
    for ptxn in lck_rec['ptxns']:
      d.dbg ("ptxn:{}".format(ptxn))
      rec_ptxn_ukeys.append(ptxn.split(':')[1])
      d.dbg ("rec_ptxn_ukeys:{}".format(rec_ptxn_ukeys))

    # loop over logs for rec (need 2idx on log[ukey])
    d.dbg ("query'ing for {}".format(ukey))
    query = client.query(ns, set+'log')
    query.where(p.equals('ukey', ukey))
    query_recs = query.results( {'total_timeout':2000})
    d.dbg ("query_recs:{}".format(query_recs))
    for query_rec in query_recs: # query_recs is a list of kmr recs
      d.dbg ("query_rec:{}".format(query_rec))
      log_rec = query_rec[2]
      d.dbg ("log_rec:{}".format(log_rec))
      # pending txns get deducted from total log amount
      d.dbg ("log_rec['txnid']:{} rec_ptxn_ukeys:{}".format(log_rec['txnid'], rec_ptxn_ukeys))
      if log_rec['txnid'] in rec_ptxn_ukeys:
        pend_amt += int(log_rec['amt'])
        d.dbg ("pend_amt:{}".format(pend_amt))
      else:
        log_amt += int(log_rec['amt'])
        d.dbg ("log_amt:{}".format(log_amt))

    lck.release()

    # report reconciliation errors
    balanced = False
    if rec_bal == log_amt:
      d.dbg ("rec_bal == log_amt")
      balanced = True
    else:
      rcn_fails.append(ukey)
    d.inf ("ukey:{} bal:{} pend_amt:{} log_amt:{} balanced:{}".format(ukey, rec_bal, pend_amt, log_amt, balanced))

    if rec_bal != log_amt:
      d.inf ("{} Not Balanced!\n  scan_rec:{}\n  lck_rec:{}".format(ukey, scan_rec, lck_rec))

  lck_fails = []
  rcn_fails = []
  if ukey is not None:
    kmr = get_hndlr (client, ns, set, ukey, d=d)
    get_recs (kmr)
  else:
    scan_hndlr (client, ns, set, get_recs, d=d)

  if len(lck_fails) > 0:
    d.dbg ("Failed to acquire lock on ukeys:{}".format(lck_fails))

  if len(rcn_fails) > 0:
    d.dbg ("Failed to reconcile ukeys:{}".format(rcn_fails))

  d.leave ()
  return {'lck_fails': lck_fails, 'rcn_fails': rcn_fails}

  # rcn_hndlr


def cln_callback (client, ns, set, ukey, txnid, d=None):

  if d is None: d = Dbg('none')
  d.enter ("ns:{} set:{} ukey:{} txnid:{}".format(ns, set, ukey, txnid))

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if txnid is None: errs += "txnid is req'd; "
  if errs: d.throw (ValueError, errs)

  # remove transaction log record
  log_ukey = ukey + ":" + txnid
  d.inf ("deleting txn log record w/ key:{}".format(log_ukey))
  rc = del_hndlr (client, ns, set+'log', log_ukey, no_exist_allowed=True, d=d)
  if rc < 0:
    d.dbg ("Failed to remove {}.{}.{}".format(ns,set+'log',log_ukey))
    d.leave (-1)
    return -1

  d.leave (0)
  return 0

  # cln_callback


def idx_hndlr (client, ns, set, ukey=None, d=None):

  if d is None: d = Dbg('none')
  d.enter ()

  # check args
  errs = ''
  if ns is None: errs += "ns is req'd; "
  if set is None: errs += "set is req'd; "
  if ukey is None: errs += "ukey is req'd; "
  if errs: d.throw (ValueError, errs)

  # get a record from the set

  d.leave (0)
  return 0


def chk_func_args (dct, reqd):
  errs = ''
  for req in reqd:
    if dct[req] is None:
      errs += "arg {} req'd; ".format(req)
  if errs:
    func = inspect.currentframe().f_back.f_code.co_name
    errs = "Error: {}; {}".format(func,errs)
  return errs


def chk_args (args, req_lst):

  emsgs = []
  argv = vars(args)
  for req in req_lst:
    if argv[req] is None:
      if req == 'cmd':
        cmds_str = ''
        for cmd in cmds:
          cmds_str += " {}".format(cmd)
        emsgs.append ("'{}' argument required.\n  One of: {}".format(req,cmds_str))
      else:
        emsgs.append ("'{}' argument required.".format(req))

  # Show usage
  if len(emsgs) != 0:
    # special arg list to generate usage; allows usage w/o debug switch listings
    main_parser(['usage', emsgs])
    #return -1
    sys.exit (1)

  return 0

  # chk_args


def main ():

  args = main_parser(sys.argv)

  # create dbg object
  d = Dbg (lvl              = args.dbg_level,
           trc              = args.dbg_trace,
           log_file         = args.dbg_log_file,
           log_only         = args.dbg_log_only,
           date_n_time      = args.dbg_date_n_time,
           file_n_line      = args.dbg_file_n_line,
           hdr_date_format  = args.dbg_hdr_date_format,
           file_name        = args.dbg_file_name,
           func_name        = args.dbg_func_name,
           line_begin       = args.dbg_line_begin,
           line_end         = args.dbg_line_end)

  d.enter ()

  global fpct

  cmd = args.cmd
  ns = args.ns
  set = args.set
  ukey = args.ukey
  txnid = args.txnid
  rec = args.rec
  ops = args.ops
  amt = args.amt
  cnt = args.cnt
  fpct = args.fpct
  nx = args.nx
  gen = args.gen
  host = args.host
  port = args.port

  chk_args (args, ['cmd'])

  # Create a basic ops list
  basic_ops = [
    operations.read ("name"),
    operations.read ("bal"),
    operations.read (Lck.lck_bin),
    operations.read (Lck.timeout_bin),
    operations.read (Lck.pending_txns_bin)
  ]

  # Connect to asdb; always req'd
  client = aero_connect ({'hosts': [(host, port)]}, d=d)
  if client is None:
    d.err ("Failed connect")
    d.leave (-1)
    return -1

  # Get help
  if cmd == 'help':
    import mxn
    d.out ("{}".format(help(mxn)))
    d.out ("\n\n")
    # d.out ("{}".format(help(Lck)))
    import aslib.asutils
    d.out ("{}".format(help(aslib.asutils)))
    pass

  # Get a list of namespaces
  elif cmd == 'info':
    chk_args (args, ['ukey'])
    ns_lst = info_hndlr (client, ukey, d=d)
    d.out ("{}".format(json.dumps(ns_lst,sort_keys=True)))

  # Put a record
  elif cmd == 'put':
    chk_args (args, ['ns', 'set', 'ukey', 'rec'])
    rc = put_hndlr (client, ns, set, ukey, rec, nx, gen, d=d)
    if rc < 0:
      d.err ("Failed to put {}.{}.{} {}".format(ns,set,ukey,rec))

  # Put multiple random records
  elif cmd == 'mputs':
    chk_args (args, ['ns', 'set', 'ukey', 'cnt'])
    mputs_hndlr (client, ns, set, ukey, cnt, d=d)

  # Get a record
  elif cmd == 'get':
    chk_args (args, ['ns', 'set', 'ukey'])
    loop_cnt = cnt
    if loop_cnt is None: loop_cnt = 1
    for i in range(loop_cnt):
      if ukey.isnumeric():
        ukey = int(ukey)
      (rkey, meta, rec) = get_hndlr (client, ns, set, ukey, d=d)
      if rec is None:
        d.err ("Failed to get ({}:{}:{})".format(ns,set,ukey))
      else:
        #d.out ("{}".format(json.dumps(rec,sort_keys=True)))
        d.out ("{}".format(rec))
    # # get_record w/ field spec
    # lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
    # if ops is None:
    #   ops = '''[
    #     "lck",
    #     "lck.txnid",            # "txnid" field from "lck" map
    #     "ptxns",
    #     "ptxns.1"               # 2nd elm of array
    #    ]'''
    # (rkey, rmeta, rec) = lck.get_record (rec_fields=eval(ops))
    # d.out ("fields:{}".format(json.dumps(rec,sort_keys=True)))

  # Run operate command
  elif cmd == 'op':
    chk_args (args, ['ns', 'set', 'ukey'])
    if ops is None: ops = basic_ops
    (rkey, meta, rec) = operate_hndlr (client, ns, set, ukey, ops, d=d)
    if rec is None:
      d.err ("Failed operate on ({}:{}:{})".format(ns,set,ukey))
    else:
      d.out ("{}".format(json.dumps(rec,sort_keys=True)))

  # Scan a set
  elif cmd == 'scan':
    chk_args (args, ['ns', 'set'])
    # scan result foreach callback takes a (key,meta,rec) tuple
    global agg
    agg = 0
    def txnid_amt (kmr):
      global agg
      try:
        rec = kmr[2]
        if set == 'mxn':
          name = rec['name']
          bal = rec['bal']
          try:
            lck = rec['lck']
            lckto = rec['lckto']
            ptxns = rec['ptxns']
          except:
            lck = '{}'
            lckto = 0
            ptxns = '[]'
          d.out ("{}\"name\" :\"{}\", \"bal\" :\"{}\", \"lck\" :\"{}\", \"lckto\" :\"{}\", \"ptxns\" :\"{}\" {}".format('{', name, bal, lck, lckto, ptxns, '}'))
        if set == 'mxnlog':
          txnid = rec['txnid']
          amt = rec['amt']
          ukey = rec['ukey']
          uset = rec['set']
          agg += int(amt)
          d.out ("{}\"txnid\": \"{}\", \"amt\" :\"{}\", \"ukey\" :\"{}\", \"set\" :\"{}\"{}".format('{', txnid, amt, ukey, uset, '}'))
      except Exception as x:
        xmsg = "txnid_amt: log_scan: rec:{} Exception: {}".format(rec,x)
        d.exc (x, xmsg, lvl=d.ERR)
    def print_rec (kmr):
      try:
        key = kmr[0]
        d.dbg ("{}".format(key))
        rec = kmr[2]
        d.out ("{}".format(rec))
        #d.out ("{}".format(json.dumps(rec,sort_keys=True,indent=2)))
      except Exception as x:
        xmsg = "print_rec: log_scan: rec:{} Exception: {}".format(rec,x)
        d.exc (x, xmsg, lvl=d.ERR)
    if set == '*':
      # dmf: add "-n 0" to just show counts
      ns_lst = info_hndlr (client, 'sets', d=d)
      #d.out ("ns_lst:{}".format(ns_lst))
      for key in ns_lst.keys():
        #d.out ("key:{}".format(key))
        #d.out ("{}".format(ns_lst[key][1]))
        ns_set_specs = ns_lst[key][1]
        for ns_set_spec in ns_set_specs.split(';'):
          if ns_set_spec == '\n': break
          #d.out ("ns_set_spec:'{}'".format(ns_set_spec))
          attrs = ns_set_spec.split(':')
          #d.out("attrs:{}".format(attrs))
          ns_discovered = attrs[0].split('=')[1]
          set_discovered = attrs[1].split('=')[1]
          objects = attrs[2].split('=')[1]
          tombstones = attrs[3].split('=')[1]
          memory_data_bytes = attrs[4].split('=')[1]
          truncate_lut = attrs[5].split('=')[1]
          stop_writes_count = attrs[6].split('=')[1]
          disable_eviction = attrs[7].split('=')[1]
          if ns_discovered == ns:
            d.out ("ns:{} set:{} objs:{}".format(ns_discovered,set_discovered,objects))
            if cnt is None:
              scan_hndlr (client, ns_discovered, set_discovered, print_rec, d=d)
        break
    else:
      # dmf: use set spec of '' to scan whole namespace with set of None
      scan_hndlr (client, ns, set, 
                  #lambda kmr: d.out ("{}".format(kmr[2])),
                  #lambda kmr: d.out ("{}".format(json.dumps(kmr[2],sort_keys=True))),
                  #txnid_amt,
                  print_rec,
                  d=d)
    # if set == 'mxnlog':
    #   d.out ("agg:{}".format(agg))

  # Delete a record
  elif cmd == 'del':
    chk_args (args, ['ns', 'set', 'ukey'])
    del_hndlr (client, ns, set, ukey, no_exist_allowed=True, d=d)

  # Truncate a set
  elif cmd == 'trunc':
    chk_args (args, ['ns', 'set'])
    trunc_hndlr (client, ns, set, d=d)

  # Lock a record w/ multi-trans locking model
  elif cmd == 'lck':
    chk_args (args, ['ns', 'set', 'ukey', 'txnid'])
    if ops is None: ops = basic_ops
    try:
      lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
      (rkey, rmeta, rec) = lck.acquire (ops)
      d.out ("{}".format(json.dumps(rec,sort_keys=True)))
      d.dbg ("rmeta:{}".format(rmeta))
    except Exception as x:
      xmsg = "Failed to acquire lock; {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)

  # Unlock a record - forced! - does not honor multi-trans locking model
  elif cmd == 'uck':
    chk_args (args, ['ns', 'set', 'ukey', 'txnid'])
    if ops is None: ops = basic_ops
    # lck = Lck (client, ns, set, ukey, 'mxn.uck', d=d, fpct=fpct)
    lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
    try:
      (rkey, rmeta, rec) = lck.break_lock (ops, ignore_timeout=True)
      d.out ("{}".format(json.dumps(rec,sort_keys=True)))
      d.dbg ("rmeta:{}".format(json.dumps(rmeta,sort_keys=True)))
    except Exception as x:
      xmsg = "Failed to acquire lock; {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)

  # Cleanup all stale pending transactions on any record
  elif cmd == 'cln':
    chk_args (args, ['ns', 'set'])
    lck = Lck (client, ns, set, d=d, fpct=fpct)
    usr_ops = None
    lck.cleanup (cln_callback, usr_ops)

  # Cleanup service; just loops forever calling cleanup
  elif cmd == 'clnsvc':
    chk_args (args, ['ns', 'set'])
    lck = Lck (client, ns, set, d=d, fpct=fpct)
    usr_ops = None
    while True:
      beg = now()
      d.dbg ("beg:{}".format(beg))
      try:
        lck.cleanup (cln_callback, usr_ops)
        end = now()
        d.dbg ("end:{} diff:{}".format(end, end-beg))
        time.sleep(1000/1000) # 100 ms
      except:
        break;

  # Clear a stale lock; destructive action; no cleanup performed
  elif cmd == 'clr':
    chk_args (args, ['ns', 'set', 'ukey', 'txnid'])
    if ops is None: ops = basic_ops
    lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
    try:
      (rkey, rmeta, rec) = lck.clear_lock (ops)
      d.out ("{}".format(json.dumps(rec,sort_keys=True)))
      d.dbg ("rmeta:{}".format(json.dumps(rmeta,sort_keys=True)))
    except Exception as x:
      xmsg = "Failed clearing lock; {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)

  # Drive a transaction - lock, transact, unlock - account settlement use case
  elif cmd == 'txn':
    chk_args (args, ['ns', 'set', 'ukey', 'txnid', 'amt'])
    (rkey, meta, rec) = txn_hndlr (client, ns, set, ukey, txnid, amt, d=d)
    d.out ("{}".format(json.dumps(rec,sort_keys=True)))

  # Drive random transactions
  elif cmd == 'rtxns':
    chk_args (args, ['ns', 'set', 'cnt'])
    rtxns_hndlr (client, ns, set, ukey, cnt, d=d)
    #d.out (Lck.get_stats())
    try:
      Lck.put_stats(client, ns, set)
    except Exception as x:
      xmsg = "Failed putting lock stats; {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)

  # Aggregate log amounts (to reconcile logs w/ accounts)
  elif cmd == 'rcn':
    chk_args (args, ['ns', 'set'])
    fails = rcn_hndlr (client, ns, set, ukey, d=d)
    if len(fails['lck_fails']) != 0:
      d.out ("Failed to lock some records: {}".format(fails['lck_fails']))
    if len(fails['rcn_fails']) == 0:
      if len(fails['lck_fails']) != 0:
        d.out ("Reconciled; except for failed locks")
      else:
        d.out ("Reconciled")
    else:
      d.out ("Unbalanced records found: {}".format(fails['rcn_fails']))

  # Create 2nd idx
  elif cmd == 'idx':
    chk_args (args, ['ns', 'set', 'ukey'])
    rc = idx_hndlr (client, ns, set, ukey, d=d)
    if rc < 0:
      d.out ("Failed to create 2nd index on {} bin".format(ukey))
    else:
      d.out ("2nd index created on {} bin".format(ukey))

  # Test driver
  elif cmd == 'tst':
    try:
      lck = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
      lck.acquire()
      lck.release()
    except Exception as x:
      xmsg = "Failed lock acquire/release; {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)
    #
    try:
      lck2 = Lck (client, ns, set, ukey, txnid, d=d, fpct=fpct)
      lck2.acquire()
      lck2.release()
    except Exception as x:
      xmsg = "Failed lock #2 acquire/release; {}".format(x)
      d.exc (x, xmsg, lvl=d.ERR)
    #
    d.out (lck.get_stats())
    ###
    # chk_args (args, ['ns', 'set'])
    # def mxn_scan (kmr):
    #   try:
    #     rec = kmr[2]
    #     name = rec['name']
    #     bal = rec['bal']
    #     lck = rec['lck']
    #     ptxns = rec['ptxns']
    #     #d.out ("name:{} bal:{}".format(name, bal))
    #     for ptxn in ptxns:
    #       #d.out ("  ptxn: {}".format(ptxn))
    #       d.out ("{} bal:{}".format(ptxn, bal))
    #   except Exception as x:
    #     xmsg = "mxn_scan: Exception: {}".format(x)
    #     d.exc (x, xmsg, lvl=d.ERR)
    # def mxnlog_scan (kmr):
    #   try:
    #     rec = kmr[2]
    #     ukey = rec['ukey']
    #     txn = rec['txn']
    #     amt = txn['amt']
    #     txnid = rec['txnid']
    #     d.out ("{} amt:{}".format(ukey+":"+txnid, amt))
    #   except Exception as x:
    #     xmsg = "mxnlog_scan: Exception: {}".format(x)
    #     d.exc (x, xmsg, lvl=d.ERR)
    # scan_hndlr (client, ns, set, mxnlog_scan, d=d)
    

  # Disconnect from asdb; always req'd
  try:
    client.close()
  except:
    pass
  d.dbg ("Disconnected from aerospike cluster")

  d.leave (0)
  return 0

  # main

# allows for 'import mxn' and standard Python help(mxn) call
if __name__ == "__main__":

  main()


