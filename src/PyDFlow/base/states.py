'''
@author: Tim Armstrong
'''
#==============#
# TASK STATES  #
#==============#
T_INACTIVE, T_DATA_WAIT, T_DATA_READY, T_CONTINUATION, \
        T_QUEUED, T_RUNNING, \
        T_DONE_SUCCESS, T_ERROR = range(8)

task_state_name = {
    T_INACTIVE:'T_INACTIVE',
    T_DATA_WAIT:'T_DATA_WAIT',
    T_DATA_READY: 'T_DATA_READY',
    T_CONTINUATION: 'T_CONTINUATION',
    T_QUEUED: 'T_QUEUED',
    T_RUNNING: 'T_RUNNING',
    T_DONE_SUCCESS: 'T_DONE_SUCCESS',
    T_ERROR: 'T_ERROR'}

#=================#
# CHANNEL STATES  #
#=================#
CH_CLOSED, CH_CLOSED_WAITING, \
        CH_OPEN_W, CH_OPEN_RW, CH_OPEN_R, CH_DONE_FILLED, CH_DONE_DESTROYED, \
        CH_ERROR, CH_REPLACED = range(9)

channel_state_name = {
        CH_CLOSED: 'CH_CLOSED',
        CH_CLOSED_WAITING: 'CH_CLOSED_WAITING',
        CH_OPEN_W: 'CH_OPEN_W',
        CH_OPEN_RW: 'CH_OPEN_RW',
        CH_OPEN_R: 'CH_OPEN_R',
        CH_DONE_FILLED: 'CH_DONE_FILLED',
        CH_DONE_DESTROYED: 'CH_DONE_DESTROYED',
        CH_ERROR: 'CH_ERROR',
        CH_REPLACED: 'CH_REPLACED'}
#TODO: garbage collection state: destroy the data if no output tasks depend
# on it?

#================================#
# Channel modes for prepare call #
#================================#
M_READ, M_WRITE, M_READWRITE = range(3)
