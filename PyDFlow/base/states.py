#==============#
# TASK STATES  #
#==============#
T_INACTIVE, T_DATA_WAIT, T_DATA_READY, T_QUEUED, T_RUNNING, \
        T_DONE_SUCCESS, T_ERROR = range(7)

#=================#
# CHANNEL STATES  #
#=================#
CH_CLOSED, CH_CLOSED_WAITING, \
        CH_OPEN_W, CH_OPEN_RW, CH_OPEN_R, CH_DONE_FILLED, CH_DONE_DESTROYED, \
        CH_ERROR = range(8)

#TODO: garbage collection state: destroy the data if no output tasks depend
# on it?

#================================#
# Channel modes for prepare call #
#================================#
M_READ, M_WRITE, M_READWRITE = range(3)
