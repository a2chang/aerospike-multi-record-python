#!/usr/bin/env python3

import sys
import os
import datetime
import inspect


#
# TODO:
# 
# create config file
#   basename ${0}/.dbg ?
#   ~/.dbg ?
#

#
# Utility to write debug lines to stdout and, optionally, to a log file
# 
# The 'out' method is called to write the lines
#
class Dbg:

  """
  Utility to write debug lines to stdout and/or to a log file.
  
  Example:
    import dfutils.dbg as dbg
    d = dbg.Dbg (lvl              = "err",
                 log_file         = "app.log",
                 log_only         = False,
                 force_out        = True,
                 date_n_time      = False,
                 file_n_line      = True,
                 hdr_date_format  = "%Y-%m-%d %H:%M:%S",
                 file_func_line   = False)
    d.wrn ("print at 'wrn' level, or above")

  Where:
    lvl         Specifies the level of output to generate when using this 
                object.  Values include: 'none', 'any', 'out', 'err',
                'wrn', 'inf', 'dbg', and 'dbg2'.  All coded debug lines
                configured with the specified 'lvl', or lower will be
                output at runtime - default level: 'err'.
    trc         Enables printing of enter/leave lines and appropriate 
                indentation of all output lines
    log_file    If specified causes any output to be appended to the named 
                log file - default: None.
    log_only    Bypass stdout and only write debug output to the specified 
                log_file - default: False.
    force_out   Forces 'out' level lines to be displayed even in log_only 
                mode - default: True.  Useful for displaying, and
                logging regular output, but not displaying debug output
                to terminal.
    date_n_time Prepend date and timestamp header to each line of output -
                default: False.
    file_n_line Prepend code file and line number to each line of output -
                default: True.
    hdr_date_format  Format for date_n_time header -
                default: "%Y-%m-%d %H:%M:%S.%f" # includes milli seconds.
    file_func_line Format d.msgs() output to include the file, func, and line
    fnm         file name filter; only named file msgs printed
    func        func name filter; only named func msgs printed; comma delimited list
    ln_beg      line filter; only lines within beg:end printed
    ln_end
  """

  # output levels for callers
  NONE=-1; ANY=-1; OUT=0; ERR=1; WRN=2; INF=3; DBG=4; DB2=5; IDB=6; LOG=7

  # output levels to allow for conditional display of debug messages
  levels = {
    "none":-1, "any":-1, "out":0, "err":1, "wrn":2, "inf":3, "dbg":4,
    "db2":5, "idb":6, "log":7
  }

  lvl_dflt = "err"
  dlvl_dflt = ERR
  trc_dflt = False

  script = None             # calling script name
  dmsgs = []                # debug message list
  dlvl = ERR                # debug level (int)
  trc = False               # print call stack enter/leave strings
  log_fp = None             # log file handle; if logging enabled
  log_file = None           # log file name
  log_only = False          # don't write stdout
  force_out = True          # force 'out' level even if log_only
  date_n_time = True        # add date and timestamp header to each line
  file_n_line = True        # add file and lineno header to each line
  hdr_date_format = "%Y-%m-%d %H:%M:%S.%f" # includes milli seconds
  file_func_line = False    # file, func, and line formating on msgs()
  fnm = None                # file name filter; only named file msgs printed
  func = None               # func name filter; only named func msgs printed
  ln_beg = None             # line filter; only lines within beg:end printed
  ln_end = None             # can limit to a single dbg msgs line

  # add msg to dmsgs list; saves lvl, fnm, func, ln, and txt
  def msg(self,dlvl,txt):
    # get file name, func name, and line number
    if dlvl < self.ANY or dlvl > self.LOG:
      raise ValueError("Debug level of '{}' not recognized.".format(dlvl))
    lvl = list(self.levels.keys())[list(self.levels.values()).index(dlvl)]
    try:
      fnm = inspect.currentframe().f_back.f_globals['__file__']
    except:
      fnm = "<nofile>"
    fnm = os.path.realpath(fnm)
    fnm = os.path.basename(fnm)
    func = inspect.currentframe().f_back.f_code.co_name
    ln = inspect.currentframe().f_back.f_lineno
    dpth = self.__frame_depth(less=1)
    # allows for filtering of msg output by lvl, fnm, func, and ln range
    mobj = {'lvl':lvl,'fnm':fnm,'func':func,'ln':ln,
            'txt':txt,'dlvl':dlvl,'dpth':dpth}
    self.dmsgs.append(mobj)



  def msgsx(self,msg=None):
    if msg: self.msg (self.ERR, msg)
    self.msgs(); sys.exit(1)



  # handle messages; cleared by default; ffl: prepend file:func():line(dpth)
  def msgs(self,banner=None, dlvl=None, fnm=None, func=None, ln_beg=None,
           ln_end=None, ffl=None, clr=True):
    if ffl is None: ffl = self.file_func_line
    if banner:
      self.__print (self.OUT,banner)
    for msg in self.dmsgs:
      if dlvl is not None and msg['dlvl'] > dlvl: continue
      if fnm is not None and msg['fnm'] != fnm: continue
      if func is not None and msg['func'] != func: continue
      if ln_beg is not None and msg['ln'] < ln_beg: continue
      if ln_end is not None and msg['ln'] >= ln_end: continue
      txt = msg['txt']
      if ffl:
        txt = msg['fnm']+':'+msg['func']+'():'+str(msg['ln'])+\
              '('+str(msg['dpth'])+') '+msg['lvl']+" "+msg['txt']
      pdlvl = msg['dlvl']
      if dlvl is not None: pdlvl = dlvl
      self.__print (pdlvl,txt,dpth=msg['dpth'])
    if clr:
      self.dmsgs.clear()



  # True if msgs in dmsgs list
  def errs (self):
    if len(self.dmsgs) > 0: return True
    else: return False



  def clr (self):
    self.dmsgs.clear()



  # show call stack
  def stack(self,banner=None):
    dpth = 1
    stk = []
    while True:
      try:
        frm = sys._getframe(dpth)
        fnm,ln,func,coc,inx = inspect.getframeinfo(frm)
        fnm = os.path.realpath(fnm)
        fnm = os.path.basename(fnm)
        sfrm = "{}, {}(), ln:{}".format(fnm,func,ln)
        stk.append(sfrm)
        dpth += 1
      except ValueError:
        print ("stack: {}".format(banner))
        for elm in reversed(stk):
          print ("  "+elm)
        return str(stk)



  # determine indentation for sub-functions
  def __frame_depth(self,less=None):
    dpth = 2  # current frame and caller's frame always exist
    if less is not None: dpth += less # add'l caller frames
    while True:
      try:
        sys._getframe(dpth)
        dpth += 1
      except ValueError:
        return dpth - 1  # subtract current frame


  # format a line for output; optionally add a header
  def __format_line(self, dlvl, txt):

      hdr = ""

      # add date and timestamp to line header
      if self.date_n_time:
        hdr += "%s %s" %(
          datetime.datetime.now().strftime(self.hdr_date_format),
          list(self.levels.keys())[list(self.levels.values()).index(dlvl)])

      # get file name, func name, and line number
      try:
        fnm = inspect.currentframe().f_back.f_back.f_back.f_globals['__file__']
      except:
        fnm = "<nofile>"
      fnm = os.path.realpath(fnm)
      fnm = os.path.basename(fnm)
      func = inspect.currentframe().f_back.f_back.f_back.f_code.co_name
      ln = inspect.currentframe().f_back.f_back.f_back.f_lineno

      # add filename, [func] and line number to line header
      if self.file_n_line:
        func = "__main__:" if func == "<module>" else func+"():"
        if self.date_n_time:
          hdr += " "
        hdr += "%s:%s%s" %(fnm, func, ln)

      # format header
      if self.file_n_line or self.date_n_time:
        hdr = "[%s] " %hdr

      return "%s%s\n" %(hdr, txt)


  # filter lines by filenm, func, ln_beg, ln_end
  def __filter (self, dpth):

      hdr = ""

      # get file name, func and line number; at depth
      frame = inspect.currentframe().f_back
      while dpth > 0:
        try:
          fnm = frame.f_globals['__file__']
        except:
          fnm = "<nofile>"
        fnm = os.path.realpath(fnm)
        fnm = os.path.basename(fnm)
        func = frame.f_code.co_name
        ln = frame.f_lineno
        frame = frame.f_back
        dpth -= 1

      if self.fnm is not None and fnm != self.fnm: return True
      if self.func is not None:
        for fnc in self.func.split(','):
          if func == fnc: return False
        return True
      if self.ln_beg is not None and int(ln) < int(self.ln_beg): return True
      if self.ln_end is not None and int(ln) > int(self.ln_end): return True

      return False


  # show func enter and leave msgs at 'dbg' level
  def enter(self, msg=""): self.__enter(msg)

  # get deeper in stack so __format() works correctly for all
  def __enter(self, msg=""):
    if not self.trc:
      return
    if self.__filter (3):
      return
    dpth = self.__frame_depth()
    func = inspect.currentframe().f_back.f_back.f_code.co_name
    ws = "  " * (dpth - 4)
    ln = "%s(%s) {" %(func,msg)
    #if self.dlvl <= self.INF and func[0] != '_':
    #  sys.stdout.write ("{} {}\n".format(func,msg))
    if not self.log_only:
      sys.stdout.write (ws + ln + "\n")
    if self.log_fp:
      self.log_fp.write (self.__format_line (self.DBG, ln))


  # show func enter and leave msgs at 'dbg' level
  def leave(self, msg=""): self.__leave(msg)

  # get deeper in stack so __format() works correctly for all
  def __leave(self, msg=""):
    if not self.trc:
      return
    if self.__filter (3):
      return
    dpth = self.__frame_depth()
    func = inspect.currentframe().f_back.f_back.f_code.co_name
    lnum = inspect.currentframe().f_back.f_back.f_lineno
    ws = "  " * (dpth - 4)
    ln = "} # %s:%s %s" %(func,lnum,msg)
    if not self.log_only:
      sys.stdout.write (ws + ln + "\n")
    if self.log_fp:
      self.log_fp.write (self.__format_line (self.DBG, ln))


  # output a debug line; optionally log it, too
  def __print(self, dlvl, txt, dpth=None):

    if self.__filter (3): return

    if self.log_fp:
      self.log_fp.write (self.__format_line (dlvl, txt))

    #if not self.log_only or (self.force_out and dlvl <= self.OUT):
    if not self.log_only:
      #if self.dlvl >= self.DBG:
      if self.trc and not self.log_only:
        ws_dpth = self.__frame_depth() - 3 # legacy d.<lvl>() calls
        if dpth is not None: ws_dpth = dpth - 2
        ws = "  " * ws_dpth
      else:
        ws = ""
      sys.stdout.write (ws + txt + "\n")


  def any(self, txt):
    if self.dlvl >= self.ANY: self.__print (self.ANY,txt)
  def out(self, txt):
    if self.dlvl >= self.OUT: self.__print (self.OUT,txt)
  def err(self, txt):
    if self.dlvl >= self.ERR: self.__print (self.ERR,txt)
  def wrn(self, txt):
    if self.dlvl >= self.WRN: self.__print (self.WRN,txt)
  def inf(self, txt):
    if self.dlvl >= self.INF: self.__print (self.INF,txt)
  def dbg(self, txt):
    if self.dlvl >= self.DBG: self.__print (self.DBG,txt)
  def db2(self, txt):
    if self.dlvl >= self.DB2: self.__print (self.DB2,txt)
  def log(self, txt):
    if self.dlvl >= self.LOG: self.__print (self.LOG,txt)
  def exc(self, x, msg, xit=False, lvl=None):
    if lvl is None:
      lvl = self.DBG
    # self.dbg (msg)
    if self.dlvl >= lvl: self.__print (lvl,msg)
    msgs = x.args[0]
    if isinstance(msgs,tuple):
      for msg in msgs:
        # self.dbg (str(msg))
        if self.dlvl >= lvl: self.__print (lvl,str(msg))
    else:
      # self.dbg (str(x))
      if self.dlvl >= lvl: self.__print (lvl,str(x))
    if xit:
      sys.exit(1)


  if True: # set False to eliminate exception catching
    _Exception_ = Exception
    _KeyError_ = KeyError
    _RuntimeError_ = RuntimeError
    _TypeError_ = TypeError
    _ValueError_= ValueError
  else:
    _Exception_ = IndexError
    _KeyError_ = IndexError
    _RuntimeError_ = IndexError
    _TypeError_ = IndexError
    _ValueError_= IndexError

  def throw(self, x, msg=None, y=None, nl=False):
    if not msg:
      if not y:
        raise x
      else:
        raise x from y
    else:
      etyp, eobj, etb = sys.exc_info()
      if etyp:
        fnm = os.path.split(etb.tb_frame.f_code.co_filename)[1]
        ln = etb.tb_lineno
        #print ("dmf: {} {} {}".format(etyp,fnm,ln))
      else:
        try:
          fnm = inspect.currentframe().f_back.f_globals['__file__']
        except:
          fnm = "<nofile>"
        fnm = os.path.realpath(fnm)
        fnm = os.path.basename(fnm)
        ln = inspect.currentframe().f_back.f_lineno
      func = inspect.currentframe().f_back.f_code.co_name
      if self.dlvl >= self.INF:
        msg = msg+" [{}, {}(), ln:{}] ".format(fnm,func,ln)
      if self.dlvl >= self.DBG and nl:
        #self.__print (self.DBG, msg+" [{} throw]".format(ln))
        self.__print (self.DBG, msg)
      if not nl:
        self.__leave(msg=msg+" [{} throw]".format(ln))
      if y:
        ## ya0 = "{}, {}, {}() ln:{}".format(y.args[0],fnm,func,ln)
        ya0 = y.args[0]
        #print ("dmf: ya0:{}".format(ya0))
        if isinstance(ya0,tuple):
          ylst = list(ya0)
          ylst.append(msg)
        else:
          ylst = [ya0,msg]
        mtpl = tuple(ylst)
        raise x(mtpl)
      else:
        raise x(msg)


  def except_msg(self, msg=None, e=None):
    etyp, eobj, etb = sys.exc_info()
    if etyp:
      fnm = os.path.split(etb.tb_frame.f_code.co_filename)[1]
      ln = etb.tb_lineno
      #print ("dmf: {} {} {}".format(etyp,fnm,ln))
    else:
      try:
        fnm = inspect.currentframe().f_back.f_globals['__file__']
      except:
        fnm = "<nofile>"
        fnm = os.path.realpath(fnm)
        fnm = os.path.basename(fnm)
        ln = inspect.currentframe().f_back.f_lineno
    func = inspect.currentframe().f_back.f_code.co_name
    if not msg:
      msg = ''
    if self.dlvl >= self.INF:
      msg = msg+" [{}, {}(), ln:{}]".format(fnm,func,ln)
    if e:
      msg += "; {}".format(e)
    if self.dlvl >= self.DBG:
      #self.__print (self.DBG, msg+" [{} throw]".format(ln))
      self.__print (self.DBG, msg)
    return msg


  # interactive debugger
  def idb(self, txt=None):

    #if self.dlvl < self.IDB:
    #  return

    help_str = """
      Python debugger
      
      h(help) command
      w(here)
      d(own) [count] # newer frame
      u(p) [count] # older frame
      b(reak) [([filename:]lineno | function) [, condition]]
      tbreak [([filename:]lineno | function) [, condition]] # temporary
      cl(ear) [filename:lineno | bpnumber [bpnumber ...]]
      s(tep)
      n(ext)
      r(eturn)
      c(ontinue)
      l(ist)
      p expression
      interact # start interpreter with current globals() and locals()
      q(uit)
    """
    import pdb
    print ("Python debugger\n{}",format(help_str))
    pdb.set_trace()


  # set debug level from string
  def setLvl(self, slvl):
    self.dlvl = self.levels[slvl]

  # set debug level from int
  def setDlvl(self, dlvl):
    self.dlvl = dlvl

  # set debug trace
  def setTrc(self, on=True):
    self.trc = on

  # set log_file
  def setLogFile(self, log_file):
    self.log_file = log_file
    self.log_fp = open (log_file, 'a')


  # set log_fp
  def setLogFp(self, log_fp):
    if self.log_fp:
      self.log_fp.close()
    self.log_fp = log_fp


  # set log_only
  def setLogOnly(self, log_only):
    self.log_only = log_only


  # set force_out
  def setForceOut(self, force_out):
    self.force_out = force_out


  # set date_n_time
  def setDateNTime(self, date_n_time):
    self.date_n_time = date_n_time


  # set file_n_line
  def setFileNLine(self, file_n_line):
    self.file_n_line = file_n_line


  # set hdr_date_format
  def setHdrDateFormat(self, hdr_date_format):
    self.hdr_date_format = hdr_date_format


  # get debug level
  def getLvl(self):
    return list(self.levels.keys())[list(self.levels.values()).index(self.dlvl)]


  # get debug trace
  def getTrc(self):
    return self.trc


  # get log_file
  def getLogFile(self):
    return self.log_file


  # get log_fp
  def getLogFp(self):
    return self.log_fp


  # get log_only
  def getLogOnly(self):
    return self.log_only


  # get force_out
  def getForceOut(self):
    return self.force_out


  # get date_n_time
  def getDateNTime(self):
    return self.date_n_time


  # get file_n_line
  def getFileNLine(self):
    return self.file_n_line


  # get hdr_date_format
  def getHdrDateFormat(self, hdr_date_format):
    return self.hdr_date_format


  # define command line parser
  def arg_parser(parent=None):

    import argparse

    class SmartFormatter(argparse.HelpFormatter):
      def _split_lines(self, text, width):
        if text.startswith('PPrint:'):
          return text[7:].splitlines()  
        # this is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)

    # define parent command line parser to pass to client cmd line parser
    if parent:
      parser = argparse.ArgumentParser (parents=[parent], add_help=False,
                                        formatter_class=SmartFormatter)
    else:
      parser = argparse.ArgumentParser (add_help=False,
                                        formatter_class=SmartFormatter)

    # level
    parser.add_argument (
      "-dlvl", "--dbg_level", metavar="level",
      default=list(Dbg.levels.keys())[list(Dbg.levels.values()).index(Dbg.dlvl)],
      choices=Dbg.levels,
      help="Enable debug output at specified level. " +
      "Allowed values are: [" + 
      ", ".join(sorted(Dbg.levels, key=Dbg.levels.get)) + "].")

    # trace
    parser.add_argument (
      "-dtrc", "--dbg_trace", action='store_true',
      default=Dbg.trc,
      help="Display function enter/leave strings.")

    # log file
    parser.add_argument (
      "-dlog", "--dbg_log_file", metavar="log_file",
      default=Dbg.log_file,
      help="Write debug output to named logfile.")

    # log_only
    parser.add_argument (
      "-dlgo", "--dbg_log_only", action='store_true',
      default=Dbg.log_only,
      help="Supress terminal output; only write to logfile.")

    # date_n_time
    parser.add_argument (
      "-dldt", "--dbg_date_n_time", action='store_false',
      default=Dbg.date_n_time,
      help="Don't prepend date and time header to log lines.")

    # file_n_line
    parser.add_argument (
      "-dlfl", "--dbg_file_n_line", action='store_false',
      default=Dbg.file_n_line,
      help="Don't prepend file name and line number header to log lines.")

    # hdr_date_format
    parser.add_argument (
      "-dlhf", "--dbg_hdr_date_format", metavar="hdr_date_format",
      default=Dbg.hdr_date_format,
      help="Header date format [\"%%Y-%%m-%%d %%H:%%M:%%S\"].")

    # filenm
    parser.add_argument (
      "-dfnm", "--dbg_file_name", metavar="dbg_file_name",
      default=Dbg.fnm,
      help="Filter output by named file.")

    # func
    parser.add_argument (
      "-dfunc", "--dbg_func_name", metavar="dbg_func_name",  
      default=Dbg.func,
      help="Filter output by named func.")

    # ln_beg
    parser.add_argument (
      "-dlnb", "--dbg_line_begin", metavar="dbg_line_begin",
      default=Dbg.ln_beg,
      help="Filter output by line number begin and/or end.")

    # ln_end
    parser.add_argument (
      "-dlne", "--dbg_line_end", metavar="dbg_line_end",
      default=Dbg.ln_end,
      help="Filter output by line number begin and/or end.")

    return parser


  # initialize object instance
  def __init__(self, lvl=lvl_dflt, trc=trc_dflt, log_file=None, log_only=None,
               force_out=None, date_n_time=None, file_n_line=None,
               hdr_date_format=None, file_func_line=None, file_name = None,
               func_name = None, line_begin = None, line_end = None ):

    # handy to have around
    self.script = os.path.basename(sys.argv[0])

    # set debug level
    try:
      self.dlvl = self.levels[lvl]
    except KeyError as e:
      raise ValueError("Debug level of '{}' not recognized.".format(lvl))

    # set trace
    self.trc = trc

    # open log_file; gets closed in __exit__()
    if log_file != None:
      self.log_file = log_file
      self.log_fp = open (log_file, 'a')

    if log_only != None:
      self.log_only = log_only

    if force_out != None:
      self.force_out = force_out

    if date_n_time != None:
      self.date_n_time = date_n_time

    if file_n_line != None:
      self.file_n_line = file_n_line

    if log_only == True and log_file == None:
      raise ValueError("Can't specify log_only and no log_file!")

    if hdr_date_format != None:
      self.hdr_date_format = hdr_date_format

    if file_func_line != None:
      self.file_func_line = file_func_line

    if file_name != None:
      self.fnm = file_name

    if func_name != None:
      self.func = func_name

    if line_begin != None:
      self.ln_beg = line_begin

    if line_end != None:
      self.ln_end = line_end

  def __enter__(self):
    return self


  def __exit__(self, exc_type, exc_value, traceback):
    if self.log_fp != None:
      self.log_fp.close()


# test driver
if __name__ == "__main__":

  def samples(d):
    d.out ("out test")
    d.wrn ("warning test")
    d.err ("error test")
    d.inf ("info test")
    d.dbg ("debug test")


  print ()
  print ("Debug level 'dbg', file_n_line, date_n_time, /tmp/tst.log")
  d = Dbg('dbg', file_n_line=True, date_n_time=True, log_file="/tmp/tst.log")
  samples(d)
  del d

  print ()
  print ("Debug level 'wrn', file_n_line, date_n_time")
  d = Dbg('wrn', file_n_line=True, date_n_time=True)
  samples(d)
  del d

  print ()
  print ("Debug level 'inf', date_n_time")
  d = Dbg('inf', date_n_time=True)
  samples(d)
  del d

  print ()
  print ("Debug level 'inf', date_n_time")
  d = Dbg('inf', date_n_time=True)
  d.setHdrDateFormat("%Y-%m-%d %H:%M:%S")
  samples(d)
  del d

  print ()
  print ("Debug level 'err', file_n_line")
  d = Dbg('err', file_n_line=True)
  samples(d)
  del d

  print ()
  print ("Debug level 'dbg', file_n_line")
  d = Dbg('dbg', file_n_line=True)
  samples(d)
  del d

  print ()
  print ("Debug level 'dbg', file_n_line, /tmp/tst.logonly, log_only, force_out")
  d = Dbg('dbg', file_n_line=True, log_file="/tmp/tst.logonly", log_only=True, 
          force_out=True)
  samples(d)
  del d

  print ()
  print ("Debug level 'dbg', file_n_line, log_only, force_out")
  try:
    d = Dbg('dbg', file_n_line=True, log_only=True, 
            force_out=True)
    samples(d)
    del d
  except ValueError as e:
    print ("Caught exception: %s" %e)


