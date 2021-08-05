#!/usr/bin/env python3

import inspect
import json
import os
import random
import sys
import time
import uuid

import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import operations
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike import predicates as p

# test driver in this file is w/in aslib hierarchy
if __name__ == "__main__":
  from dbg import Dbg
else:
  from aslib.dbg import Dbg

# 
# Locking a record adds bins to the record.
# 
#   lck'd rec:   {"lck": {"txnid": "<user_txnid>", "ukey": "<user_ukey>"}, 
#                 "lckto": <now()+Lck.duration>, 
#                 "ptxns": ["<ukey:txnid>"]}
# 
#   unlck'd rec: {"lck": {}, "ptxns": [], "lckto": 0}
# 

# TODO:

# 
# finish documentation
# 
# shld run ntp on clients as well as server
#
# chk for txnid in addition to gen; gen number wraps
# 
# chk for txnid in map before applying transaction
#   allows for replay on failure
#
# shld timeout reset on retry
# 
# acquire, release, break_lock, clear lock
#  acquire - lck map keys not exist
#    no lock - acquire it
#    txnid eq lock - steal it - match: gen, txnid; ignore: timeout
#    timedout lock - steal it - match: gen, timeout; ignore: txnid
#  release - match gen; ensures all
#  break_lock - match: gen, timeout; ignore: txnid; leave ptxns[] alone
#  clear - match: gen, timeout; ignore: txnid; clear pxtns[]
# 


# current time in ms
now = lambda: int(round(time.time() * 1000))


__doc__ = '''
The Lck class provides pessimistic, cooperative locking in support of
multi-record transactional writes to the Aerospike database.

Aerospike 'operate' actions can be supplied to each of the
Lck.acquire(), Lck.release(), Lck.break_lock(), and Lck.cleanup()
methods, to be executed against the locked record.  This provides
greater efficiency through fewer transactions against the database. Any
write that occurs to the locked record outside of these supplied usr_ops
arguments must be driven through the Lck.operate() method, as that
tracks the record's generation number, which is essential for
successful, eventual release of the lock.

Several lock configuration parameters exist allowing users to alter lock
behavior, as follows.

  duration = 100                # Lock duration in ms
  grace_period = 200            # 200ms grace
  retry_cnt = 3                 # Number of times to attempt to acquire lock
  retry_delay = 30              # Delay in ms between retries

These values should only be modified via the Lck constructor call, and
not by direct manipulation of the class member variables.  Though Python
does not prevent users modifying these, or other member values, it is
strongly recommened that this not be done.


Following is an example script utilizing this locking class.

  from __future__ import print_function
  import aerospike
  from aerospike_helpers.operations import operations
  from aslib.asutils import Lck
  import uuid
  import sys

  host = '127.0.0.1'
  port = 3000
  ns = 'test'
  set = 'lck_tst'
  log_set = 'log_tst'
  ukey = 'tst0'
  txnid = str(uuid.uuid4())
  amt = 100

  # Connect to Aerospike server
  try:
    cfg = {'hosts': [(host, port)]}
    client = aerospike.client(cfg).connect()
  except:
    print ("Failed Aerospike connect to host '{}'".format(host+":"+port))
    sys.exit(-1)

  # Lock an account record
  try:
    lck = Lck (client, ns, set, ukey, txnid)
    kmr = lck.acquire ()
  except:
    print ("Failed locking record '{}'".format(ns, set, ukey))
    sys.exit(-1)

  # Write transaction to log
  try:
    key = (ns, log_set, ukey+":"+txnid)
    rec = {"txnid": txnid, "amt": amt, "set": set, "ukey": ukey}
    client.put(key, rec)
  except:
    print ("Failed logging transaction")
    lck.break_lock ()   # N.B. Not lck.release(); that wld clear pending txn
    sys.exit(-1)        # not that it matters as the txn log wasn't written
                        # still, might be nice to see that failure before cleanup

  # Settle transaction while releasing the lock
  usr_ops = [ operations.increment("bal", amt) ]
  kmr = lck.release (usr_ops)   # clears txnid from pending list
  if kmr[0] is None:
    print ("Failed settling transaction and releasing record")
    sys.exit(-1)

  # Disconnect from Aerospike server
  client.close()

  print ("Settled transaction amount of {} on record ({}:{}:{})"\
         .format(amt, ns, set, ukey))

  sys.exit(0)


Locks may fail in several ways:

  - acquire (e.g. record currently locked, non-existent, network errors)
  - release (e.g. network errors)
  - timeout (e.g. user process dies)
  - user code errors

Successfully acquired locks log the user's transaction Id within a list
of pending transactions in a bin within the locked record.  Upon
successful release of the lock, the user's transaction Id is removed
from that list.

In the event of failure, as described above, the transaction Id is not
removed from the pending transaction list.  This provides a means for a
user defined cleanup process to reconcile any partially executed
multi-record transactions.  The Lck.cleanup() method takes a callback
function argument which is called to execute the user defined cleanup
code.  That code must be idempotent, as failures can result during the
cleanup process.  For example, the user's cleanup callback function may
execute, but the removal of the transaction Id from the pending list may
fail.  In such cases, subsequent cleanup runs will re-execute the user's
callback upon the same transaction Id.  This simply can not be avoided.

It should be noted that when errors are encountered by user code, that
code should not call the Lck.release() method, but, instead, call the
Lck.break_lock() method.  This ensures that the failed transaction Id
remains within the pending list for later cleanup.

Failed locks are automatically reclaimed.  Two reclaimation processes
exist.  The first is to simply break an expired lock.  Locks are
considered expired when their timeout time, plus their configured
grace_period have been exceeded.  A lock created with the default
configurations will expire 200ms after it was created, and may be
reclaimed.

The second way that locks may be reclaimed is in the event that an
additional thread, or process is started to complete a user transaction
that seems to have stalled, or failed; whether it has, or not, is
immaterial.  In the event that a lock acquire attempt is made against a
live lock with the same transaction Id, the lock is taken by the new
caller.  This will cause the original holder of the lock to fail
when attempting any subsequent Lck operation - Lck.release(),
Lck.break_lock(), or Lck.operate() - due to a lock record generation
match error.  If this scenario is possible, the user transaction code
must be idempotent, as two process may, potentially, execute the user
side tranasaction code in duplicate.  Note that there is no way for the
original lock holder to impact successful execution of the new lock
holders lock record related actions.

The ClientWrapper class provides a simple wrapper around the Aerospike
client handle passed to the Lck constructor.  The wrapper allows for
random failures prior to, and after any call to the Aerospike database.
The failure percentage argument to the Lck constructor indicates the
percentage of failures that should occur with any call.  The fpct value
is in 10ths of a percent, so that 10 represents 1%, and 1 represents 0.1%.
'''
class Lck:

  # caller provided values
  client = None                 # Aerospike client
  ns = None                     # Nameset
  set = None                    # Set name
  ukey = None                   # User key
  txnid = None                  # Lock transaction Id

  # configurable values
  duration = 100                # Lock duration in ms
  grace_period = 200            # 200ms grace
  retry_cnt = 3                 # Number of times to attempt to acquire lock
  retry_delay = 30              # Delay in ms between retries

  # generated values
  timeout = None                # Epoch; generated; now() + duration
  gen = None                    # Lock record generation; saved and managed
  err = None                    # Error string; instance; overwritten each call

  # for testing
  fpct = 0                      # Failure percentage (0 to 100)

  # defines
  lck_bin = "lck"               # Lock bin name
  ukey_mkey = "ukey"            # Lock ukey map key
  txnid_mkey = "txnid"          # Transaction Id map key
  timeout_bin = "lckto"         # Lock timeout (epoch); handy for 2nd idx
  pending_txns_bin = "ptxns"    # Pending transactions bin name
  sole_pending = False          # True if lck is only pending txn
  cleanup_prefix = 'ptxns_cleanup' # cleanup process txnid prefix
  gen_bin = "gen"               # generation number bin from returned meta
  stats_bin = "totals"          # optional stats bin
  stats_attempted_mkey = "attempted" # total locks attempted
  stats_acquired_mkey = "acquired" # total locks acquired
  stats_released_mkey = "released" # total locks released
  stats_broken_mkey = "broken"     # total locks broken
  stats_timedout_mkey = "timedout" # total locks timedout
  stats_stolen_mkey = "stolen"     # total locks stolen
  stats_duration_mkey = "duration" # total duration over all locks
  stats_retries_mkey  = "retries"  # total retries over all locks

  # instance stats
  tm_acquired = 0
  tm_released = 0

  # class stats
  lcks_attempted = 0
  lcks_acquired = 0
  lcks_released = 0
  lcks_broken = 0
  lcks_stolen = 0
  lcks_duration = 0
  lcks_timedout = 0
  lcks_stolen = 0
  lcks_retries = 0

  acquire_policy =  {           # Write policy for lock acquisition
    'map_order': aerospike.MAP_KEY_VALUE_ORDERED,
    'map_write_flags': aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
  }

  steal_policy =  {             # Write policy for stealing a lock
    'map_order': aerospike.MAP_KEY_VALUE_ORDERED,
    'gen': aerospike.POLICY_GEN_EQ
  }

  release_policy =  {           # Write policy for lock release
    'map_order': aerospike.MAP_KEY_VALUE_ORDERED,
    'gen': aerospike.POLICY_GEN_EQ
  }

  def __acquire_ops (self, txnid, timeout):
    '''generate operate list for lock acquisition'''
    return [
      map_operations.map_put (self.lck_bin, self.txnid_mkey, txnid, self.acquire_policy),
      map_operations.map_put (self.lck_bin, self.ukey_mkey, self.ukey, self.acquire_policy),
      operations.write (self.timeout_bin, timeout),
      list_operations.list_append (self.pending_txns_bin, self.ukey+":"+txnid, None),
      operations.read (self.lck_bin),
      operations.read (self.timeout_bin),
      operations.read (self.pending_txns_bin)
    ]


  def __steal_ops (self, txnid, timeout):
    '''generate operate list for stealing a lock'''
    return [
      map_operations.map_put (self.lck_bin, self.txnid_mkey, txnid, self.steal_policy),
      map_operations.map_put (self.lck_bin, self.ukey_mkey, self.ukey, self.steal_policy),
      operations.write (self.timeout_bin, timeout),
      # tnxid shld already be in the pending list
      #list_operations.list_append (self.pending_txns_bin, self.ukey+":"+txnid, None),
      operations.read (self.lck_bin),
      operations.read (self.timeout_bin),
      operations.read (self.pending_txns_bin)
    ]


  def __replace_ops (self, txnid, timeout):
    '''generate operate list for replacing a lock'''
    return [
      map_operations.map_put (self.lck_bin, self.txnid_mkey, txnid, self.steal_policy),
      map_operations.map_put (self.lck_bin, self.ukey_mkey, self.ukey, self.steal_policy),
      operations.write (self.timeout_bin, timeout),
      list_operations.list_append (self.pending_txns_bin, self.ukey+":"+txnid, None),
      operations.read (self.lck_bin),
      operations.read (self.timeout_bin),
      operations.read (self.pending_txns_bin)
    ]


  def __release_ops (self):
    '''generate operate list for releasing a lock'''
    if self.sole_pending:
      return [
        operations.write (self.timeout_bin, aerospike.null()),
        operations.write (self.lck_bin, aerospike.null()),
        operations.write (self.pending_txns_bin, aerospike.null()),
        operations.read (self.lck_bin),
        operations.read (self.timeout_bin),
        operations.read (self.pending_txns_bin)
      ]
    else:
      return [
        operations.write (self.timeout_bin, aerospike.null()),
        operations.write (self.lck_bin, aerospike.null()),
        list_operations.list_remove_by_value (self.pending_txns_bin, self.ukey+":"+self.txnid, aerospike.LIST_RETURN_NONE),
        operations.read (self.lck_bin),
        operations.read (self.timeout_bin),
        operations.read (self.pending_txns_bin)
      ]


  def __break_ops (self):
    '''generate operate list for breaking a lock'''
    return [
      operations.write (self.lck_bin, aerospike.null()),
      operations.write (self.timeout_bin, aerospike.null()),
      operations.read (self.lck_bin),
      operations.read (self.timeout_bin),
      operations.read (self.pending_txns_bin)
    ]

  def __clear_ops (self):
    '''generate operate list for clearing a lock'''
    return [
      operations.write (self.pending_txns_bin, aerospike.null()),
      operations.read (self.lck_bin),
      operations.read (self.timeout_bin),
      operations.read (self.pending_txns_bin)
    ]


  def acquire (self, usr_ops=None):

    '''

    Acquire a lock on a record
    
    Arguments
    
      usr_ops          user operations; optional
    
    Return values
    
      (rkey, rmeta, rec)   tuple describing the locked record
                           the rec value includes the three lock bins
                           and any values provided by the user operations list
      raises exception     upon failue to acquire the lock
    
    Variables
    
      txnid            transaction id; from lock object
      duration         lock duration; defaulted, or user provided (ms)
      timeout          lock timeout epoch; now() + duration (ms)
      grace_period     post timeout recovery delay (ms)
      acquire_ops      operations run to create lock; plus usr_ops
      acquire_policy   write policy; MAP_WRITE_FLAGS_CREATE_ONLY
      retry_cnt        number of attempts; defaulted, or user provided
      gen              record generation; saved upon success
    
    Description
    
    Locking a record involves writing three bins to the record:
    
      lck_bin            { txnid: tid, ukey: ukey }
      timeout_bin        timeout
      pending_txns_bin   [ txnid ]
    
    The records are written with a policy of MAP_WRITE_FLAGS_CREATE_ONLY
    ensuring that the update is written only if there are no map keys on
    the lck_bin map value, thus establishing the lock.  No other lock
    attempt may succeed until the current lock is released by clearing
    the lck_bin map keys.
    
    In addition to the lock_bin value, a timeout value is written to
    the timeout_bin.  This value is used to recover stale locks.  A 
    secondary index may be created upon the timeout_bin to enable efficient
    recovery and cleanup of stale locks by driving a query where the 
    lock timeout value, plus a grace_period, is checked against the current
    time.  That said, locks are automatically recovered once timed out.
    In addition, locks attempted on records already locked with the same
    transaction Id are simply taken, causing the previous lock holder to
    fail upon any subsequent Lck method invocation.
    
    The pending_txns_bin is used to log the active transaction id.  In the 
    event of a stale lock, the recovery process will leave the pending_txns_bin
    intact.  A cleanup process can then discover failed transactions and
    perform requisite cleanup operations.  This process must be a user
    driven cleanup process executed via the Lck.cleanup method.
    
    User operations upon the locked record may be executed along with
    the lock acquisition step.  This is both efficient, and
    recommended.  Any modifications made to the locked record must be
    made either via the usr_ops argument to one of the Lck methods, or
    via the Lck.operate method, as record generation counts must be
    maintained, or Lck methods will fail.  This ensures integrity of the
    locking implementaiton.
    
    Lock retries are attempted in the event of failure to acquire the
    lock.  Three scenarios present:
    
      locked       the record has a valid, active lock
      expired      the lock has timed out and can be reclaimed
      duplicate    the transaction Id matches the current lock
    
    Retries are executed with a policy requiring the record's
    generation number to match that discovered when querying for the
    record's status.  This ensures that no clashes occur when
    recovering an existing lock.
    
    If an existing, live lock matches the transaction Id of the
    attempted lock, then the lock is stolen.  For a second thread or
    process to attempt to acquire a lock on the same record with the
    same unique transaction id it is assumed that there was some issue
    with the existing thread or process holding that lock.  It is thus
    safe to recover the lock.  Should the original thread or process
    attempt to proceed it will fail during any Lck method call due to a
    record generation mismatch.  Any transaction operations provided by
    the user at that time will also fail.  It is also assumed that any
    user operations driven on other unlocked records are idempotent.
    
    If the existing lock is simply expired, taking into account an
    appropriate grace period, then the lock is also stolen.  Again,
    generation matching is enforced.
    
    In either event, the existing transaction id is not removed from
    the pending transaction list.  This allows for a user driven cleanup
    process to discover stale transaction Ids in the pending list and
    perform any appropriate cleanup on the database.
    
    A sleep delay is provided between lock acquisition attempts.  This
    value is defaulted, or provided by the user.  In addition, when
    discovering the record's current lock status, its timeout plus a
    grace period is used to calculate an appropriate wait time for the
    retry delay.  The wait time is the difference between the current
    time and current lock's expiration time plus grace period.  The
    lesser of the wait time and retry delay time is used for the sleep
    operation.

    '''

    d = self.d
    d.enter ("usr_ops:{}".format(usr_ops))

    # check args
    errs = ''
    if self.ukey is None: errs += "ukey is req'd; "
    if self.txnid is None: errs += "txnid is req'd; "
    if errs: self.err = errs; d.throw (ValueError, errs)
  
    self.__stats (lck_attempted=True)

    self.timeout = now() + self.duration

    # Set ops for lock acquisition
    ops = []
    for op in self.__acquire_ops(self.txnid, self.timeout):
      ops.append (op)

    # add user ops to ops list
    if usr_ops is not None:
      for op in usr_ops:
        ops.append (op)

    meta = None
    policy = self.acquire_policy

    d.db2 ("ops:{}".format(ops))

    timedout = False
    stolen = False

    retry = 0
    while retry < self.retry_cnt:

      try:

        d.dbg ("attempting to lock {}:{}:{} txnid:{} gen:{}"\
               .format(self.ns, self.set, self.ukey, self.txnid, self.gen))

        key = (self.ns, self.set, self.ukey)
        (rkey, rmeta, rec) = self.client.operate (key, ops, meta, policy)

        if rec is None:
          emsg = "Error: Failed acquiring lock on {}".format(key)
          d.leave (emsg)
          raise Exception (emsg)

        # check for single ptxns list entry (ours) and flag that
        # allows us to remove the 'ptxns' bin on release, rather than remove
        # our txnid from the list
        if len(rec[self.pending_txns_bin]) == 1:
          self.sole_pending = True

        self.__stats (tm_acquired=self.timeout-self.duration,
                    lck_timedout=timedout, lck_stolen=stolen, num_retries=retry)

        self.gen = rmeta['gen']

        d.dbg ("Lock attempt on key:{} for txnid:{} succeeded after {} tries"\
               .format(key, self.txnid, retry))

        break # from retry loop w/ retry < retry_cnt shows success!

      except Exception as x:

        # may have been a steal/time last round, but don't know about next yet
        timedout = False
        stolen = False

        if type(x) == ex.ElementExistsError:

          d.dbg ("Lock attempt #{} failed {}".format(retry, x.msg))

          # could be a stale lock; attempt to steal/take it
          #   get record and check tnxid and timeout
          #   if our txnid, steal lock; stale/dead thread/process w/ same task?
          #   if timed out (+grace_period), take lock
          #   if we succeed, any other thread/process will fail on gen match
          #   N.B. but it cld still do non-idempotent txn work on other rec's
          #     though, they'll fail to update lock record when done
          #     the new process will manage txn and lck
          #     there will be no issue, assuming user txn is idempotent

          # get current record to check its status; specify fields to return
          # rec: { name: nm, balance: bal,
          #        lck: { txnid: tid, ukey: ky},
          #        lckto: to, ptxns: [tid1, tid2] } 
          rec_fields = [
            # lck.txnid - rec: { lck.txnid: tid }
            self.lck_bin+"."+self.txnid_mkey,
            # lckto - rec: { lckto: tm }
            self.timeout_bin
            # result - rec: { lck.txnid: tid, lckto: tm  }
          ]
          meta_fields = ['gen']

          try:
            key = (self.ns, self.set, self.ukey)
            (rkey, rmeta, rec) = self.client.get(key)
            d.dbg ("rec:{} rmeta:{}".format(rec,rmeta))
          except Exception as x:
            xmsg = "Exception: get'ing record; {}".format(x)
            d.leave (xmsg)
            raise Exception (xmsg)

          # extract gen, txnid, and timeout from returned metadata
          record_gen = None
          record_txnid = None
          record_timeout = None
          try:
            record_gen = rmeta['gen']
            record_txnid = rec[Lck.lck_bin][Lck.txnid_mkey]
            record_timeout = rec[Lck.timeout_bin]
            d.dbg ("record_gen:{} record_txnid:{} record_timeout:{}"\
                   .format(record_gen, record_txnid, record_timeout))
          except:
            if record_gen is None:
              xmsg = "Exception on record_gen; strange!; {}".format(x)
              d.leave (xmsg)
              raise Exception (xmsg)
            if record_txnid is None or record_timeout is None:
              d.dbg ("Stale lock check found it had been freed")
              retry +=1
              continue

          # steal locks with same txnid - match: gen, txnid; ignore: timeout
          #   our txnid already in ptxns, so use steal ops
          if record_txnid == self.txnid:
            d.dbg ("Current lock has our txnid; attempting to steal it")
            meta = {'gen': record_gen}
            ops = []
            for op in self.__steal_ops(self.txnid, self.timeout):
              ops.append (op)
            if usr_ops is not None:
              for op in usr_ops:
                ops.append (op)
            retry += 1
            stolen = True
            continue

          # set wait time
          wait_time = record_timeout + self.grace_period - now()
          d.dbg ("wait_time:{} now:{}".format(wait_time, now()))

          # if wait_time is longer than expected lock duration, punt
          #   honor the grace period
          if wait_time > self.duration:
            emsg = "Wait time too long on key:{}".format(self.ukey)
            d.leave (emsg)
            raise Exception (emsg)

          # replace expired locks - match: gen, timeout; ignore: txnid
          if wait_time <= 0: 
            emsg = "Current lock has expired; attempting to steal it"
            d.dbg (emsg)
            meta = {'gen': record_gen}
            ops = []
            for op in self.__replace_ops(self.txnid, self.timeout):
              ops.append (op)
            if usr_ops is not None:
              for op in usr_ops:
                ops.append (op)
            retry += 1
            timedout = True
            continue

          # try again - sleep for shorter of retry_delay or wait_time
          sleep_time = self.retry_delay/1000
          if wait_time < sleep_time:
            sleep_time = wait_time
          d.dbg ("sleep({})".format(sleep_time))
          time.sleep(sleep_time)

          retry += 1
          continue

        else:
          emsg = "Lock Exception; {}".format(x)
          d.leave (emsg)
          raise Exception (emsg)
          
    # retry_cnt exceeded
    if retry >= self.retry_cnt:
      emsg = "Lock attempt on key:{} for txnid:{} failed after {} tries"\
             .format(key, self.txnid, retry)
      d.leave (emsg)
      raise Exception (emsg)
  
    d.leave ((None, rmeta, json.dumps(rec)))
    return (rkey, rmeta, rec)

  # acquire


  def release (self, usr_ops=None, break_lock=False):

    '''
    
    Release a lock on a record
    
    Arguments 
    
     usr_ops          user operations; optional
     break_lock       if true, break the lock, rather than release it
                      a broken lock retains its pending transaction
                      a released lock has its pending transaction cleared
     
    Return values
     
     (rkey, rmeta, rec)   tuple describing the released record
                          the rec value includes the three lock bins
                          and any values provided by the user operations list
     raises exception     upon failue to acquire the lock
     
    Variables
     
     gen              generation number to match w/ record; from lock object
     release_policy   forces generation match
     
    Description
     
     In order to release the lock the following must be true:
      
      the record generation number must match the lock gen
    
     Note that the lock timeout need not have occured, nor is the the
     record transaction id checked, as that will be insured to match due
     to it having been set by the acquire call and our generation number
     matching.
     
     On success, the record will be cleared of the lck_bin values, the
     timeout_bin, and the transaction id of the lock will be cleared
     from the records pending_txns_bin.  N.B. all instances of the txnid
     will be cleared from the pending_txns_bin.
    
    '''

    d = self.d
    d.enter ("usr_ops:{}".format(usr_ops))

    # check args
    errs = ''
    if self.ukey is None: errs += "ukey is req'd; "
    if self.txnid is None: errs += "txnid is req'd; "
    if errs: d.throw (ValueError, errs)
  
    # Set ops for lock release or break
    ops = []
    if break_lock:
      # don't remove txnid from ptxns[]
      d.dbg ("adding __break_ops to ops")
      for op in self.__break_ops():
        ops.append (op)
    else:
      # do remove txnid from ptxns[]
      d.dbg ("adding __release_ops to ops")
      for op in self.__release_ops():
        ops.append (op)

    # add user ops to ops list
    if usr_ops is not None:
      d.dbg ("adding usr_ops to ops")
      for op in usr_ops:
        ops.append (op)

    d.db2 ("ops:{}".format(ops))

    meta = {'gen': self.gen}
    policy = self.release_policy

    try:
      key = (self.ns, self.set, self.ukey)
      d.dbg ("attempting to unlock key:{} txnid:{} gen:{}"\
             .format(key, self.txnid, self.gen))
      (rkey, rmeta, rec) = self.client.operate (key, ops, meta, policy)
      self.__stats (tm_released=now())

    except Exception as x:
      if break_lock:
        xmsg = "Break lock exception: {}".format(x)
      else:
        xmsg = "Release lock exception: {}".format(x)
      d.leave (xmsg)
      raise Exception (xmsg)

    d.leave ((None, rmeta, json.dumps(rec)))
    return (rkey, rmeta, rec)

  # release


  def break_lock (self, usr_ops=None, ignore_timeout=False):

    '''
    
    Break a lock on a record; same as release, but leaves pending txn
     Intended for use in error conditions.
     
    Arguments
     
     usr_ops          user operations; optional
     ignore_timeout   if true, ignore the lock's timeout value
     
    Return values
     
     (rkey, rmeta, rec)   tuple describing the broken record
                          the rec value includes the three lock bins
                          and any values provided by the user operations list
     raises exception     upon failue to acquire the lock
     
    Variables
     
     gen              generation number to match w/ record; from lock object
     duration         duration of lock; lock default, or user provided (ms)
     timeout          lock timeout epoch
     grace_period     buffer after timeout to allow for cleanup (ms)
     wait_time        time after now when current lock can be broken
     
    Description
     
     In order to break the lock the following must be true:
      
      the record lock must be timedout and the grace period expired
      the record generation number must match the lock gen
      
     The timeout check may be overridden using the appropriate flag argument.
     
     Broken locks retain their pending transaction list intact.  This
     allows for user defined cleanup process to identify failed
     transactions and perform requisite cleanup activities.
     
    Notes
     
     Clients must drive their own cleanup processes.
    
    '''

    d = self.d
    d.enter ("usr_ops:{}".format(usr_ops))

    # check args
    errs = ''
    if self.ukey is None: errs += "ukey is req'd; "
    if self.txnid is None: errs += "txnid is req'd; "
    if errs: d.throw (ValueError, errs)
  
    # get current gen and timeout values
    rec_fields = [
      self.timeout_bin,
      self.lck_bin
    ]
    meta_fields = ['gen']

    try:
      key = (self.ns, self.set, self.ukey)
      (rkey, rmeta, rec) = self.client.get(key)
      d.dbg ("rec:{} rmeta:{}".format(rec,rmeta))
    except Exception as x:
      xmsg = "Exception: get'ing record; {}".format(x)
      d.leave (xmsg)
      raise Exception (xmsg)

    # extract gen, txnid, and timeout from returned metadata
    record_gen = None
    record_txnid = None
    record_timeout = None
    try:
      record_gen = rmeta['gen']
      record_txnid = rec[Lck.lck_bin][Lck.txnid_mkey]
      record_timeout = rec[Lck.timeout_bin]
      d.dbg ("record_gen:{} record_txnid:{} record_timeout:{}"\
             .format(record_gen, record_txnid, record_timeout))
    except:
      if record_gen is None:
        xmsg = "Exception on rec gen, txnid, or lckto; strange!; {}".format(x)
        d.leave (xmsg)
        raise Exception (xmsg)
      if record_txnid is None or record_timeout is None:
        emsg = "Stale lock check found it had been freed"
        d.dbg (emsg)
        # dmf: don't return; may still need ptxns cleanup
        # d.leave (emsg)
        # return (rkey, rmeta, rec)

    # set wait time
    if record_timeout is None:
      wait_time = 0  # there was no lock, or its our lock to steal
    else:
      wait_time = record_timeout + self.grace_period - now()
    d.dbg ("wait_time:{} now:{}".format(wait_time, now()))

    # check timeout
    if not ignore_timeout and wait_time > 0:
      emsg = "Can not break lock; not expired, or grace period not exceeded."
      d.leave (emsg)
      raise Exception (emsg)

    # force gen match; harsh
    self.gen = record_gen

    # break the lock
    (rkey, rmeta, rec) = self.release (usr_ops, break_lock=True)
    d.dbg ("rmeta:{} rec:{}".format(rmeta, rec))

    d.leave ((None, rmeta, rec))
    return (rkey, rmeta, rec)

  # break_lock


  def clear_lock (self, usr_ops=None):

    '''
    
    Clear a lock; catastrophically
     This is a helper method for testing and anomalous situations.  It
     should not be called as part of any normal flow.
    
    Arguments
     
     none
     
    Return values
     
     (rkey, rmeta, rec)   tuple describing the cleared record
                          the rec value includes the three lock bins
     raises exception     upon failue to acquire the lock
     
    Variables
     
     gen              generation number to match w/ record; from lock object
     txnid            transaction id; must match current record lock
     duration         duration of lock; lock default, or user provided (ms)
     timeout          lock timeout epoch
     grace_period     buffer after timeout to allow for cleanup (ms)
     wait_time        time after now when current lock can be broken
     
    Description
     
     In order to clear the lock the following must be true:
      
      nothing!  this is a very heavy handed action meant for testing
      
     Cleared locks lose all pending transactions w/o cleanup; beware!
     
    Notes
     
     Clients must drive their own cleanup processes.
    
    '''

    d = self.d
    if self.ukey is None: errs += "ukey is req'd; "
    if self.txnid is None: errs += "txnid is req'd; "
    d.enter ()

    # check args
    errs = ''
    #if ARG is None: errs += "ARG is req'd; "
    if errs: d.throw (ValueError, errs)
  
    # set clear ops
    ops = []
    for op in self.__clear_ops():
      ops.append (op)

    # add user ops to ops list
    if usr_ops is not None:
      for op in usr_ops:
        ops.append (op)

    d.db2 ("ops:{}".format(ops))

    # break lock
    try:
      (rkey, rmeta, rec) = self.break_lock (ops, ignore_timeout=True)
      d.dbg ("rec:{}".format(json.dumps(rec)))

    except Exception as x:
      emsg = "Clear Lock exception: {}".format(x)
      d.leave (emsg)
      raise Exception (emsg)

    d.leave ((None, rmeta, rec))
    return (rkey, rmeta, rec)

  # clear_lock


  def cleanup (self, callback=None, usr_ops=None, cln_ukey=None):

    '''
    
    Cleanup stale, pending transactions from a data set, or individual record
      
      live locked records are skipped; clean 'em up next time around
      
      callback must be idempotent
        Failure to unlock record after running callback() on each ptxn
        (however that may happen; network failure, etc) will leave
        orignal ptxns list intact, but callbacks will have already been
        run on each txnid. The next cleanup round will re-execute that
        callback on those same txnid's
    
    Usage
      
      # create a lock object
      cln_lck = Lck (client, ns, set)  # ukey, txnid not req'd; ignored anyway
      
      # optionally define user callback to drive for each stale pending txn
      def cleanup_callback (client, ns, set, ukey, txnid):
        # perform requisite cleanup actions
        cfg = {'hosts': [('127.0.0.1', 3000)]}
        client = aerospike.client(cfg).connect()
        txnlog_ns = 'test'
        txnlog_set = 'txnlog'
        txnlog_key = ukey+":"+txnid
        txnlog_key = (txnlog_ns, txnlog_set, txnlog_key)
        rc = client.remove(txnlog_key)
        # handle exceptions!
        # perhaps record not found errors are okay; catch and return 0
        return rc
      
      # optionally define ops executed on cleaned up record post callback
      usr_ops = [ operations.write('clean_flg', 1) ]
      
      # run cleaup process
      cln_lck.cleanup (callback, usr_ops)
    
    '''

    d = self.d
    d.enter ()

    # check args
    errs = ''
    # if self.ukey is None: errs += "ukey is req'd; "
    # if self.txnid is None: errs += "txnid is req'd; "
    if errs: d.throw (ValueError, errs)
  
    def cleanup_pending_list (kmr):

      '''
      Sub method used to scan account records and call client's cleanup
      callback with any pending transactions.
      '''

      d.enter ("meta:{} rec:{}".format(kmr[1], kmr[2]))

      try:

        rec = kmr[2]
        try:
          ptxns = rec[self.pending_txns_bin]
        except:
          ptxns = []

        # check for pending transactions
        if len(ptxns) == 0:
          msg = "No pending transactions for record; skipping"
          d.dbg (msg)
          d.leave (msg)
          return

        # use unlocked ptxns list elm to determine record ukey
        ukey = ptxns[0].split(":")[0] # ptxns [ 'ukey:txnid', ..]
        d.dbg ("ukey:{}".format(ukey))

        self.ukey = ukey

        # lock the record
        try:
          self.txnid = Lck.cleanup_prefix+'-'+str(uuid.uuid4())
          (rkey, rmeta, rec) = self.acquire ()
        except Exception as x:
          xmsg = "Failed to lock ukey:{}; skipping record; {}".format(ukey, x)
          d.dbg (xmsg)
          d.leave (xmsg)
          return

        # pull the current ptxns list to drive cleanup from
        # it may have changed before we acquired the lock
        rec_ptxns = rec[self.pending_txns_bin]

        # manage list of ptxn ids to clear from ptxns list
        #   callback() may fail; e.g. if a record is locked, timeout, etc
        ptxns_to_clear = []

        # loop over ptxns
        for ptxn in rec_ptxns:

          d.dbg ("cleaning ptxn:{}".format(ptxn))

          # call user's callback
          if callback is not None:

            # split out pending txnid from ptxn entry
            ptxnid = ptxn.split(":")[1] # ptxn 'ukey:txnid'

            # skip cleanup lock ptxn; no cleanup necessary
            prefix = ptxnid.split("-")[0] # ptxns_cleanup-uuid
            if prefix == Lck.cleanup_prefix:
              d.dbg ("skipping cleanup ptxn; it's not been logged!")
              rc = 0

            else:
              # cleanup!
              d.dbg ("calling callback");
              rc = callback (self.client, self.ns, self.set, ukey, ptxnid, d=self.d)

            # add ptxn id to keep list
            if rc == 0:
              ptxns_to_clear.append (ptxn)
            else:
              d.dbg ("callback return error; keep ptxn:{}".format(ptxn))

        # all pending tnxs processed; clear ptxns list of those cleaned up

        # create list of ptxns to keep
        ptxns_to_keep = []
        for ptxn in rec_ptxns:
          if ptxn not in ptxns_to_clear:
            ptxns_to_keep.append (ptxn)

        # create ops to write ptxns list with uncleaned txns
        d.dbg ("ptxns_to_keep:{}".format(ptxns_to_keep))
        if len(ptxns_to_keep) == 0:
          ops = [ operations.write (Lck.pending_txns_bin, aerospike.null()) ]
        else:
          ops = [ operations.write (Lck.pending_txns_bin, ptxns_to_keep) ]

        # add user ops to ops list
        if usr_ops is not None:
          for op in usr_ops:
            ops.append (op)

        # release lock, clear ptxns bin, and execute any user ops
        try:
          d.dbg ("releasing lock ops:{}".format(ops))
          (rkey, rmeta, rec) = self.release (ops)
          d.dbg ("released rec:{}".format(rec))
        except:
          # N.B. record lock state is now potentially compromised
          # user's callback has been executed against all pending txns
          # however, pending txn list is still intact 
          # next cleanup call will re-call user's callback on the same txnid's
          # we proceed, assuming cleanup callback is idempotent
          d.wrn ("callback() executed on ptxns, but did not clear ptxns list")

      except Exception as x:
        xmsg = "cln: rec:{} Exception: {}".format(kmr[2], x)
        d.leave (xmsg)
        raise Exception (xmsg)

      d.leave ()
      return

    # cleanup entire set (scan)
    if cln_ukey is None:

      try:
        d.dbg ("Scanning records from {}.{}".format(self.ns, self.set))
        res = self.client.scan(self.ns, self.set)
        res.foreach(cleanup_pending_list)
        d.leave ()
        return

      except Exception as x:
        xmsg = "Exception: scan cleanup; {}".format(x)
        d.leave (xmsg)
        raise Exception (xmsg)

    # cleanup an individual record w/in the set
    else:

      try:
        key = (self.ns, self.set, cln_ukey)
        (rkey, meta, rec) = client.get(key)
        cleanup_pending_list ((rkey, rmeta, rec))

      except Exception as x:
        xmsg = "Exception: record cleanup; {}".format(x)
        d.leave (xmsg)
        raise Exception (xmsg)

    d.leave ()
    return

  # cleanup


  def operate (self, usr_ops, usr_meta=None, usr_policy=None):

    '''
    Call operate on a locked record.  Preserves lock's record generation count.
    '''

    d = self.d
    d.enter ("usr_ops:{} usr_meta:{} usr_policy:{}"\
             .format(usr_ops, usr_meta, usr_policy))

    # check args
    errs = ''
    if self.ukey is None: errs += "ukey is req'd; "
    if self.txnid is None: errs += "txnid is req'd; "
    if usr_ops is None: errs += "usr_ops is req'd; "
    if errs: d.throw (ValueError, errs)
  
    # check user metadata for gen
    meta = {'gen': self.gen}
    try:
      usr_gen = usr_meta['gen']
      if usr_gen != self.gen:
        emsg = "Record gen mismatch; rec:{} != expected:{}"\
               .format(usr_gen, self.gen)
        d.leave (emsg)
        raise Exception (emsg)
    except:
      # no 'gen' key on usr_meta; no action; our meta is fine
      pass

    # add aerospike.POLICY_GEN_EQ to policy
    policy = { 'gen': aerospike.POLICY_GEN_EQ }
    if usr_policy is not None:
      try:
        usr_gen = usr_policy['gen']
        new_gen = usr_gen | policy['gen']
        policy['gen'] = new_gen
      except:
        # no 'gen' key on usr_policy; no action; our policy is fine
        pass

    d.dbg ("meta:{} policy:{}".format(meta,policy))

    try:
      key = (self.ns, self.set, self.ukey)
      d.dbg ("calling operate on key:{}".format(key))
      (rkey, rmeta, rec) = self.client.operate (key, usr_ops, meta, policy)
      self.gen = rmeta['gen']
      d.dbg ("self.gen:{}".format(self.gen))

    except Exception as x:
      xmsg = "Operate exception; {}".format(x)
      d.leave (xmsg)
      raise Exception (xmsg)

    d.leave ((None, rmeta, rec))
    return (rkey, rmeta, rec)

  # operate


  def put_stats (client, ns, set):

    '''
    Called to log locking statistics for the specified namespace and
    set.  The ns and set args are catenated to form the ukey for the
    statiscs record updated with the current lock objects staticstics.
    All statics are written to the same namespace and to a set named
    'lck_stats'.
    '''

    stats_ops = [
      map_operations.map_increment (Lck.stats_bin, Lck.stats_attempted_mkey,
                                    Lck.lcks_attempted, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_acquired_mkey,
                                    Lck.lcks_acquired, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_timedout_mkey,
                                    Lck.lcks_timedout, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_stolen_mkey,
                                    Lck.lcks_stolen, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_broken_mkey,
                                    Lck.lcks_broken, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_released_mkey,
                                    Lck.lcks_released, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_duration_mkey,
                                    Lck.lcks_duration, None),
      map_operations.map_increment (Lck.stats_bin, Lck.stats_retries_mkey,
                                    Lck.lcks_retries, None),
      operations.read (Lck.stats_bin),
    ]

    try:
      key = (ns, 'lck_stats', ns+":"+set)
      meta = None
      policy = None
      (rkey, meta, rec) = client.operate(key, stats_ops, meta, policy)

    except Exception as x:
      xmsg = "Put stats exception; {}".format(x)
      d.leave (xmsg)
      raise Exception (xmsg)

    return

  # put_stats


  def get_stats ():
    '''
    Return the current lock's live statistics.
    '''
    stats = "Lck status\n lcks_acquired:{}\n lcks_timedout:{}\n lcks_stolen:{}\n lcks_released:{}\n lcks_duration:{}\n lcks_retries:{}\n".format(Lck.lcks_acquired, Lck.lcks_timedout, Lck.lcks_stolen, Lck.lcks_released, Lck.lcks_duration, Lck.lcks_retries)
    return stats


  def __stats (self,
             lck_attempted=None, tm_acquired=None, tm_released=None, 
             lck_timedout=None, lck_stolen=None, lck_broken=None,
             num_retries=None):

    '''
    Internal method to aggregate lock statistics.
    '''

    d = self.d
    d.enter ("lck_attempted:{}, tm_acquired:{}, tm_released:{}, lck_timedout:{}, lck_stolen:{}, lck_broken:{}, num_retries:{}".format(lck_attempted, tm_acquired, tm_released, lck_timedout, lck_stolen, lck_broken, num_retries))

    # gather some statistics
    if lck_attempted is not None and lck_attempted == True:
      Lck.lcks_attempted += 1
    if tm_acquired is not None:
      self.tm_acquired = tm_acquired
      Lck.lcks_acquired += 1
    if tm_released is not None:
      self.tm_released = tm_released
      Lck.lcks_released += 1
      Lck.lcks_duration += self.tm_released - self.tm_acquired
    if lck_timedout is not None and lck_timedout == True:
      Lck.lcks_timedout += 1
    if lck_stolen is not None and lck_stolen == True:
      Lck.lcks_stolen += 1
    if lck_broken is not None and lck_broken == True:
      Lck.lcks_broken += 1
    if num_retries is not None:
      Lck.lcks_retries += num_retries
    
    d.leave ()
    return

  # stats


  def __init__ (self, client, ns, set, ukey=None, txnid=None,
                duration=None, grace_period=None,
                retry_cnt=None, retry_delay=None,
                d=None, fpct=0):

    '''
    
    Lck constructor
      
     Create a lock object
    
    Arguments
     
     client           client connection to Aerospike server
     ns               namespace of record to lock
     set              set name of record to lock
     ukey=None        user key of record to lock; optional for cleanup only
     txnid=None       transaction id to be placed upon lock
     duration         default: lock duration in ms
     grace_period     default: 10s grace + 27s clock skew for SC setups
     retry_cnt        default: 3
     retry_delay      default: 30 ms
     d=None           debug (logging) object; defaults to 'err' lvl
     
    Return values
     
     a Lck object
    
    Instance Variables
     
     client
     ns
     set
     ukey=None                     # req'd for all but cleanup method
     txnid=None                    # req'd for all but cleanup method
     d=None                        # if d is None: d = Dbg('err')
     
    Class Variables
     
    defines
     lck_bin = "lck"               # Lock bin name
     ukey_mkey = "ukey"            # Lock ukey map key
     txnid_mkey = "txnid"          # Transaction Id map key
     timeout_bin = "lckto"         # Lock timeout (epoch); handy for 2nd idx
     pending_txns_bin = "ptxns"    # Pending transactions bin name
     gen_bin = "gen"               # generation number bin from returned meta
     
    generated values
     timeout = None                # Epoch; generated; now() + duration
     gen = None                    # Lock record generation; saved and managed
     
    for testing
     fpct = 0                      # Failure percentage (0 to 100)
     
    '''

    if d is None:
      d = Dbg ("err")
    self.d = d
    #d.enter ("ns:{} set:{} ukey:{}".format(ns, set, ukey))

    errs = ''
    if client is None: errs += "client is req'd; "
    if ns is None: errs += "ns is req'd; "
    if set is None: errs += "set is req'd; "
    # if ukey is None: errs += "ukey is req'd; "
    # if txnid is None: errs += "txnid is req'd; "
    if errs: d.throw (ValueError, errs)

    self.client = client
    if fpct is not None and fpct > 0:
      self.client = ClientWrapper (client, fpct=fpct)
    self.ns = ns
    self.set = set
    self.ukey = ukey
    self.txnid = txnid
    if duration is not None:
      self.duration = duration
    if grace_period is not None:
      self.grace_period = grace_period
    if retry_cnt is not None:
      self.retry_cnt = retry_cnt
    if retry_delay is not None:
      self.retry_delay = retry_delay
    if fpct is not None:
      self.fpct = fpct

    #d.leave ()

  # __init__



class ClientWrapper:

  client = None
  fpct = 0


  def operate (self, *args):

    self.random_fail (call="pre-operate")

    kwr = None
    try:
      kwr = self.client.operate (*args)
    except Exception as x:
      raise (x)

    self.random_fail (call="post-operate")

    return kwr


  def put (self, *args, meta=None, policy=None):

    self.random_fail (call="pre-put")

    try:
      self.client.put (*args, meta=meta, policy=policy)
    except Exception as x:
      raise (x)

    self.random_fail (call="post-put")

    return


  def get (self, *args):

    self.random_fail (call="pre-get")

    kwr = None
    try:
      kwr = self.client.get (*args)
    except Exception as x:
      raise (x)

    self.random_fail (call="post-get")

    return kwr


  def remove (self, *args):

    self.random_fail (call="pre-remove")

    try:
      self.client.remove (*args)
    except Exception as x:
      raise (x)

    self.random_fail (call="post-remove")

    return


  def scan (self, *args):

    self.random_fail (call="pre-scan")

    scan = None
    try:
      scan = self.client.scan (*args)
    except Exception as x:
      raise (x)

    self.random_fail (call="post-scan")

    return scan


  def close (self, *args):

    self.random_fail (call="pre-close")

    try:
      self.client.close (*args)
    except Exception as x:
      raise (x)

    self.random_fail (call="post-close")

    return


  # Generate a random number from 1 to 1000 to compare to percent value provided
  # Returns True when random number generated is <= to pct provided; iff pct>0
  def random_fail (self, pct=0, call=''):

    if pct == 0:
      pct = self.fpct

    if pct == 0:
      return

    try:
      fnm = inspect.currentframe().f_back.f_back.f_globals['__file__']
    except:
      fnm = "<nofile>"
    fnm = os.path.realpath(fnm)
    fnm = os.path.basename(fnm)
    func = inspect.currentframe().f_back.f_back.f_code.co_name
    ln = inspect.currentframe().f_back.f_back.f_lineno

    rn = random.randint(1, 1000)
    if rn <= pct:
      raise Exception("Error: Random {} failure file:{} func:{} ln:{}".format(call, fnm, func, ln))

    return


  def __init__ (self, client, fpct=0):

    self.client = client
    self.fpct = fpct


# test driver
if __name__ == "__main__":

  def main ():

    dlvl = "out"
    dtrc = False
    if len(sys.argv) > 1:
      dlvl = sys.argv[1]
    if len(sys.argv) > 2:
      dtrc = True

    #d = Dbg (lvl="dbg", trc=True)
    #d = Dbg (lvl="out", trc=False)
    d = Dbg (lvl=dlvl, trc=dtrc)
    d.enter ()

    host = "localhost"
    port = 3000
    cfg = {'hosts': [(host, port)]}
    client = aerospike.client(cfg).connect()

    ns = "test"
    set = "mxn"
    ukey = "k1"

    usr_ops = None

    # acquire lock 1
    txnid="txn1"
    lck1 = Lck (client, ns, set, ukey, txnid, d=d)
    (rkey, rmeta, rec) = lck1.acquire(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to acquire lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Acquired lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # release lock 1
    txnid="txn1"
    (rkey, rmeta, rec) = lck1.release(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to release lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Released lock for txnid:{} on ukey:{}".format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # acquire lock 2
    txnid="txn2"
    lck2 = Lck (client, ns, set, ukey, txnid, d=d)
    (rkey, rmeta, rec) = lck2.acquire(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to acquire lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Acquired lock for txnid:{} on ukey:{}".format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # attempt to acquire lock 3; should fail
    txnid="txn3"
    lck3 = Lck (client, ns, set, ukey, txnid, d=d)
    (rkey, rmeta, rec) = lck3.acquire(usr_ops)
    if not rec:
      d.out ("SUCCESS: Didn't acquire lock for txnid:{} on ukey:{}; as expected"\
             .format(txnid,ukey))
    else:
      d.out ("FAILURE: Acquired lock for txnid:{} on ukey:{}; unexpectedly"\
             .format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # acquire lock 2a
    #   simulating an alternate process/thread re-acquiring a lock for
    #   the same txnid after another process/thread has stalled, or died
    txnid="txn2"
    lck2a = Lck (client, ns, set, ukey, txnid, d=d)
    (rkey, rmeta, rec) = lck2a.acquire(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to acquire lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Re-acquired lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # release lock 2a
    txnid="txn2"
    (rkey, rmeta, rec) = lck2a.release(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to release lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Released lock for txnid:{} on ukey:{}".format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # acquire lock 4
    txnid="txn4"
    lck4 = Lck (client, ns, set, ukey, txnid, d=d)
    (rkey, rmeta, rec) = lck4.acquire(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to acquire lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Acquired lock for txnid:{} on ukey:{}".format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # acquire lock 4a
    txnid="txn4a"
    lck4a = Lck (client, ns, set, ukey, txnid, d=d)
    lck4a.grace_period = 2000 # 2s
    # sleep to let lock expire
    sleep_time = lck4a.duration+lck4a.grace_period
    d.inf ("sleep {}s to allow previous lock to expire".format(sleep_time/1000))
    time.sleep(sleep_time/1000)
    # now try the lock
    (rkey, rmeta, rec) = lck4a.acquire(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to acquire lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Acquired lock for txnid:{} on ukey:{}".format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # release lock 4a
    txnid="txn4a"
    (rkey, rmeta, rec) = lck4a.release(usr_ops)
    if not rec:
      d.out ("FAILURE: Failed to release lock for txnid:{} on ukey:{}"\
             .format(txnid,ukey))
    else:
      d.out ("SUCCESS: Released lock for txnid:{} on ukey:{}".format(txnid,ukey))
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    # cleanup txn4 from pending transactions list
    ops = [
      operations.write (lck4.pending_txns_bin, []),
      operations.read (lck4.lck_bin),
      operations.read (lck4.timeout_bin),
      operations.read (lck4.pending_txns_bin)
    ]
    (rkey, rmeta, rec) = lck4.operate (ops, None, None)

    if rec is None:
      d.out ("FAILURE: Failed to cleanup txn4 pending transactions list")
    else:
      d.out ("SUCCESS: Cleaned up txn4 pending transactions list")
      d.inf ("  rmeta:{} rec:{}".format(rmeta,rec))

    d.leave ()
    return

  main()
