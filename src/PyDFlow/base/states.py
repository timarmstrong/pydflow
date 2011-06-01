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
# IVAR STATES  #
#=================#
IVAR_CLOSED, IVAR_CLOSED_WAITING, \
        IVAR_OPEN_W, IVAR_OPEN_RW, IVAR_OPEN_R, IVAR_DONE_FILLED, IVAR_DONE_DESTROYED, \
        IVAR_ERROR, IVAR_REPLACED = range(9)

ivar_state_name = {
        IVAR_CLOSED: 'IVAR_CLOSED',
        IVAR_CLOSED_WAITING: 'IVAR_CLOSED_WAITING',
        IVAR_OPEN_W: 'IVAR_OPEN_W',
        IVAR_OPEN_RW: 'IVAR_OPEN_RW',
        IVAR_OPEN_R: 'IVAR_OPEN_R',
        IVAR_DONE_FILLED: 'IVAR_DONE_FILLED',
        IVAR_DONE_DESTROYED: 'IVAR_DONE_DESTROYED',
        IVAR_ERROR: 'IVAR_ERROR',
        IVAR_REPLACED: 'IVAR_REPLACED'}
#TODO: garbage collection state: destroy the data if no output tasks depend
# on it?

#================================#
# Ivar modes for prepare call #
#================================#
M_READ, M_WRITE, M_READWRITE = range(3)
