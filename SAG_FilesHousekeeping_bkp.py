#!/usr/bin/python
"""
@author jiesteban 

"""

VERBOSE_LOG_METRICS = False
VERBOSE_LOG_ACTIONS = True

import sys
sys.path.insert(0,'./lib')
#print(sys.path)





########## METRICS calculation

import math

def sd_calc(data):
    n = len(data)

    if n <= 1:
        return 0.0

    mean, sd = avg_calc(data), 0.0

    # calculate stan. dev.
    for el in data:
        sd += (float(el) - mean)**2
    sd = math.sqrt(sd / float(n-1))

    return sd

def avg_calc(ls):
    n, mean = len(ls), 0.0

    if n <= 1:
        return ls[0]

    # calculate average
    for el in ls:
        mean = mean + float(el)
    mean = mean / float(n)

    return mean

# data = [4, 2, 5, 8, 6]
# print("Sample Data: ",data)
# print("Standard Deviation : ",sd_calc(data))


def metrics_calc_from_filelist(filelist):
    ### List Comprehensions should be a bit more efficient
    # sizes = []
    # for _file in filelist:
    #     sizes.append(_file.getsize())
    
    sizes = [_file.getsize() for _file in filelist]

    return metrics_calc_from_filesizeslist(sizes)


def metrics_calc_from_filesizeslist(filesizeslist):
    if len(filesizeslist) == 0: return 0, 0, 0, 0, 0, 0
    
    _len = len(filesizeslist)
    _sum = sum(filesizeslist)
    _max = max(filesizeslist)
    _min = min(filesizeslist)
    _avg = avg_calc(filesizeslist)
    _sd  = sd_calc(filesizeslist)
    
    return _len, _sum, _max, _min, _avg, _sd


def get_filesizeslist(fileslist):
    # List Comprehensions should be a bit more efficient than loops but doen't allow exception handling
    # return [_file.getsize() for _file in fileslist]
    
    fsl = []
    for _file in fileslist:
        try:
            fsl.append(_file.getsize())
        except Exception as err:
            print('Error ' + str(err) + ' WHEN collecting size info from file: ' + str(_file) + '. Skipped')
    return fsl
    



########## Graphite 

import socket

graphiteSocket = None

def metrics_conn_open():
    TCP_IP = 'gb2infoinp01.corp.internal'
    TCP_PORT = 2004
    
    global graphiteSocket
    
    try:
        s = socket.socket()
        s.connect((TCP_IP, TCP_PORT))
        
        graphiteSocket = s
    except Exception as err:
        print('Error ' + str(err) + ' WHEN opening connection to Graphite. HK Script execution will follow but metrics will not be sent)')        
        graphiteSocket = None

def metrics_conn_close():
    global graphiteSocket

    if graphiteSocket is None:
        # print('Socket to Graphite is not open; Nothing to close.')
        return
    
    graphiteSocket.close()


pickle_list = []

import pickle
import struct

# def metrics_send_message(_socket, message):
#     print('Send metric message: ' + message)
#     #_socket = metrics_conn_open()
#     _socket.sendall(message)
#     #_socket.close()
    
def metrics_send_pickle():
    global pickle_list
    global graphiteSocket

    if graphiteSocket is None:
        # print('Socket to Graphite is not open; Metric list is not sent.')
        return
        
    try:     
        payload = pickle.dumps(pickle_list, protocol=2)
        header = struct.pack("!L", len(payload))
        message = header + payload
        
        if VERBOSE_LOG_METRICS:
            print('Send metric size: ' + str(len(payload)))
            print('Send metric list: ' + str(pickle_list))
            # print('Send metric message: ' + str(message))
            # print(str(*pickle_list, sep = "\n")) 
        graphiteSocket.sendall(message)
            
        # del pickle_list[:] # Useless
        pickle_list = []        # Empty list after sending
    except Exception as err:
        print('Error ' + str(err) + ' WHEN sending message to Graphite: ' + str(message))

def metrics_add(metricname, metricvalue):
    global timestamp
    global pickle_list
    
    pickle_list.append((metricname, (int(timestamp), metricvalue)))
    
    if VERBOSE_LOG_METRICS:
        print(str(metricname) 
              + ',' + metricvalue)

def metrics_add_folderinfo(_metricname_root, _len, _sum, _max, _min, _avg, _sd):
    global timestamp
    global pickle_list
    
    pickle_list.append((_metricname_root + '.no_files', (int(timestamp), _len)))
    pickle_list.append((_metricname_root + '.size_sum', (int(timestamp), _sum)))
    pickle_list.append((_metricname_root + '.size_max', (int(timestamp), _max)))
    pickle_list.append((_metricname_root + '.size_min', (int(timestamp), _min)))
    pickle_list.append((_metricname_root + '.size_avg', (int(timestamp), _avg)))
    pickle_list.append((_metricname_root + '.size_sd',  (int(timestamp), _sd) ))
    
    if VERBOSE_LOG_METRICS:
        print(str(_metricname_root) 
                + ',' + str(_len) 
                + ',' + str(_sum) 
                + ',' + str(_max) 
                + ',' + str(_min) 
                + ',' + str(_avg) 
                + ',' + str(_sd) ) 
    
def escape_metricname(string):
    string = string.replace('\\', '_')
    string = string.replace(':', '_')
    string = string.replace('/', '_')
    string = string.replace(' ', '_')

    return string



########## PROCESS functions

import tarfile
def compress_and_remove(archive, filelist):
    if len(filelist) == 0: return
    
    tgz = tarfile.open(str(archive), "w:gz")
    for _file in filelist:
        
        try:
            if VERBOSE_LOG_ACTIONS:
                # print("tgz " + str(_file.mtime) + " " + str(_file) + " into archive file " + archive)
                # print("tgz " + time.strftime('%Y%m%d_%H%M%S', time.gmtime(_file.mtime)) + " " + str(_file) + " into archive file " + archive)
                print("tgz " + time.strftime('%Y%m%d_%H%M%S', time.gmtime(_file.mtime)) + " " + str(_file))
        
            tgz.add(str(_file))
            _file.remove()
        except Exception as err:
            print('Error ' + str(err) + ' WHEN archiving file: ' + str(_file) + '. File not deleted')
            
            
    tgz.close()
    
def remove(filelist):
    for _file in filelist:
        
        try:
            if VERBOSE_LOG_ACTIONS:
                # print("rm " + str(_file.mtime) + " " + str(_file))
                print("rm " + time.strftime('%Y%m%d_%H%M%S', time.gmtime(_file.mtime)) + " " + str(_file))
            
            _file.remove()
        except Exception as err:
            print('Error ' + str(err) + ' WHEN removing file: ' + str(_file) + '. File not deleted')







from path import Path
import time
import itertools




def Housekeep(metrics_id_prefix, root_folder, DAYS_to_compress, DAYS_to_remove, other_more_specific_root_folders_list):

    #  if root_folder doesn't exists log error and return
    if not Path(root_folder).exists():
        print("root folder " + root_folder + " doesn't exist: nothing to do here")
        return 

    #### STAGE: INIT
        
    global timestamp
    
    ARCHIVE_FILENAME_PREFIX = 'Archive_'
    ARCHIVE_FILENAME_SUFFIX = '.tar.gz'
    
    time_init_folder = time.time()
    
    dirs      = {}
    ### 2 level dictionary
    # 1st level Key: Folder
    #   2nd level Key: type of info C, R, O
    #   2nd level Val: list with the info
    # 
    # sample entry:
    # dirs      = {str(Path('FakePath')): 
    #                 {'C': [Path('fake'), Path('file'), Path('list'), Path('compress')],
    #                  'R': [Path('fake'), Path('file'), Path('list'), Path('remove')],
    #                  'O': [Path('fake'), Path('file'), Path('list'), Path('others')]
    #                 }
    #             }
    
    
    # # List dir contents
    # for key,val in sorted(dir_no_files.items()):
    #     print key, "=>", val
    
    ### 2 level dictionary sample usage 
    # 
    # people = {1: {'name': 'John', 'age': '27', 'sex': 'Male'},
    #           2: {'name': 'Marie', 'age': '22', 'sex': 'Female'}}
    # 
    # print(people[1]['name'])
    # print(people[1]['age'])
    # print(people[1]['sex'])
    
    
    
    ## Calculate times
    
    # compressed files will be generated after DAYS_to_compress days
    DAYS_to_remove_tgz = DAYS_to_remove - DAYS_to_compress
    if DAYS_to_remove_tgz < 0:
        print("This is very strange: DAYS_to_remove < DAYS_to_compress. DAYS_to_compress will be useless")
        DAYS_to_remove_tgz = 0
    
    time_in_secs_to_compress   = timestamp - (DAYS_to_compress   * 24 * 60 * 60)
    time_in_secs_to_remove     = timestamp - (DAYS_to_remove     * 24 * 60 * 60)
    time_in_secs_to_remove_tgz = timestamp - (DAYS_to_remove_tgz * 24 * 60 * 60)
    
    # print("time_in_secs_to_compress  : " + str(time_in_secs_to_compress))
    # print("time_in_secs_to_remove    : " + str(time_in_secs_to_remove))
    # print("time_in_secs_to_remove_tgz: " + str(time_in_secs_to_remove_tgz))
    
    
    #### STAGE: COLLECT and load info in memory

    time_init_folder_collect = time.time()
    
    root_folder_path = Path(root_folder)

    
    # for d in root_folder_path.walkdirs(): --> iterate just on subfolders, but not the root folder
    # Iterate on root_folder and all subfolders 
    for d in itertools.chain([root_folder_path], root_folder_path.walkdirs()):
        
        nothing_to_do = False
        for o in other_more_specific_root_folders_list:
            if str(d).startswith(o):
                nothing_to_do = True
                break
        if nothing_to_do:
            print("Nothing to do for [" + str(d) + "] under [" + root_folder + "] because it is defined more specifically under its own entry in the policy")
            continue
            
        time_init_subfolder_collect = time.time()
        

        # Create here regardless there are files or not. If there is no files empty values will be stored
        if not str(d) in dirs:
            dirs[str(d)] = {'C': [], 'R': [], 'O': [] }

        for i in d.files():
            
            try:
                # if not str(i.dirname()) in dirs:
                #     dirs[str(i.dirname())] = {'C': [], 'R': [], 'O': [] }
                
                if str(i.basename()).startswith(ARCHIVE_FILENAME_PREFIX) and str(i.basename()).endswith(ARCHIVE_FILENAME_SUFFIX):
                    if i.mtime <= time_in_secs_to_remove_tgz:
                        dirs[str(i.dirname())]['R'].append(i)
                    # else: none
                elif i.mtime <= time_in_secs_to_remove:
                    dirs[str(i.dirname())]['R'].append(i)
                elif i.mtime <= time_in_secs_to_compress:
                    dirs[str(i.dirname())]['C'].append(i)
                else:
                    dirs[str(i.dirname())]['O'].append(i)
            
            except Exception as err:
                print('Error ' + str(err) + ' WHEN collecting info from file: ' + str(i) + '. Skipped')
        
        
        time_end_subfolder_collect = time.time()
        metrics_add(metrics_id_prefix + '.' + escape_metricname(d) + '._elapsedTime_Collecting',   str(time_end_subfolder_collect - time_init_subfolder_collect))

    time_end_folder_collect = time.time()
    metrics_add(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._elapsedTimeAggregated_Collecting',   str(time_end_folder_collect - time_init_folder_collect))


    #### STAGE: METRICS    

    time_init_folder_metrics_calculation = time.time()
    
    sizes_root_allC = []
    sizes_root_allR = []
    sizes_root_allO = []
    
    for dirname in dirs.keys():
        
        time_init_subfolder_metrics_calculation = time.time()
                
        # metricvalue_size_listC = [_file.getsize() for _file in dirs[dirname]['C']]
        metricvalue_size_listC = get_filesizeslist(dirs[dirname]['C'])
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listC)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._toCompress',  _len, _sum, _max, _min, _avg, _sd)
    
        metricvalue_size_listR = get_filesizeslist(dirs[dirname]['R'])
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listR)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._toRemove',    _len, _sum, _max, _min, _avg, _sd)
    
        metricvalue_size_listO = get_filesizeslist(dirs[dirname]['O'])
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listO)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._others',      _len, _sum, _max, _min, _avg, _sd)
        
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listC + 
                                                                      metricvalue_size_listR + 
                                                                      metricvalue_size_listO)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._all',         _len, _sum, _max, _min, _avg, _sd)
        
            
        sizes_root_allC += metricvalue_size_listC
        sizes_root_allR += metricvalue_size_listR
        sizes_root_allO += metricvalue_size_listO
        
        time_end_subfolder_metrics_calculation = time.time()

        metrics_add(metrics_id_prefix + '.' + escape_metricname(dirname) + '._elapsedTime_MetricsCalculation',     str(time_end_subfolder_metrics_calculation  - time_init_subfolder_metrics_calculation ))

        metrics_send_pickle()
            
    _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(sizes_root_allC)
    metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._toCompressAggregated',    _len, _sum, _max, _min, _avg, _sd)
    
    _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(sizes_root_allR)
    metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._toRemoveAggregated',      _len, _sum, _max, _min, _avg, _sd)
    
    _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(sizes_root_allO)
    metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._othersAggregated',        _len, _sum, _max, _min, _avg, _sd)
    
    _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(sizes_root_allC + 
                                                                  sizes_root_allR + 
                                                                  sizes_root_allO)
    metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._allAggregated',           _len, _sum, _max, _min, _avg, _sd)
        

    time_end_folder_metrics_calculation = time.time()
    
    metrics_add(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._elapsedTimeAggregated_MetricsCalculation',   str(time_end_folder_metrics_calculation- time_init_folder_metrics_calculation))
    
    metrics_send_pickle()
    
    
    #### STAGE: PROCESS
            
    time_init_folder_processing = time.time();
            
    # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    timestamp_format = time.strftime('%Y%m%d_%H%M%S', time.gmtime(timestamp))
    
    for dirname in sorted(dirs.keys()):
            
        time_init_subfolder_processing = time.time();
    
        
        archive_file = Path(dirname) / ARCHIVE_FILENAME_PREFIX + timestamp_format + ARCHIVE_FILENAME_SUFFIX
        compress_and_remove(archive_file, dirs[dirname]['C'])
        
        remove(dirs[dirname]['R'])
    
    
        time_end_subfolder_processing = time.time();
                
        metrics_add(metrics_id_prefix + '.' + escape_metricname(dirname) + '._elapsedTime_Processing',     str(time_end_subfolder_processing - time_init_subfolder_processing))
        

    time_end_folder_processing = time.time();

    metrics_add(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._elapsedTimeAggregated_Processing',   str(time_end_folder_processing  - time_init_folder_processing ))

    #### STAGE: CLOSING

    time_end_folder = time.time()
    
    # print("Elapsed time in secconds: " + str(time_end - time_init))
    metrics_add(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._elapsedTimeAggregated_All',  str(time_end_folder - time_init_folder))
    
    metrics_send_pickle()
    


import re

comment_pattern = re.compile(r'\s*#.*$')    
    
def skip_comments(lines):
    """
    A filter which skip/strip the comments and yield the rest of the lines

    :param lines: any object which we can iterate through such as a file
        object, list, tuple, or generator
    """
    global comment_pattern

    for line in lines:
        line = re.sub(comment_pattern, '', line).strip()
        if line:
            yield line


# if __name__ == '__main__':
#     with open('data_with_comments.csv') as f:
#         reader = csv.DictReader(skip_comments(f))
#         for line in reader:
#             print(line)
    



import glob
import csv
import os

POLICY_SUBFOLDER = 'policyFiles'
POLICY_FILE_PATTERN = 'hk.*.csv'

time_init_global = time.time()

timestamp = time_init_global  

if len(sys.argv) != 2:
    sys.exit(2)

policyFolder = Path(sys.argv[1]) 

if not policyFolder.exists():
    print('Policy folder ' + policyFolder + ' does not exist')
    sys.exit(1)


scriptFolder = Path(__file__).dirname()
policyFolderRoot = scriptFolder / POLICY_SUBFOLDER

if not policyFolderRoot.exists():
    print('Policy folder root ' + policyFolderRoot + ' does not exist')
    sys.exit(1)


if policyFolder.startswith(policyFolderRoot):
    print('Policy folder ' + policyFolder + ' is not under Policy folder root ' + policyFolderRoot)
    sys.exit(1)


all_policy_entries = []

for p in policyFolderRoot.walk(POLICY_FILE_PATTERN):
    
    # print "###### " + p
    with open(p, 'rb') as csvfile:
        csvreader = csv.reader(skip_comments(csvfile), delimiter=',', quotechar='"')
        all_policy_entries += list(csvreader)

#print ('all_policy_entries: ' + str(all_policy_entries))


metrics_conn_open()

os.chdir(policyFolder)

for csvfilename in glob.glob(POLICY_FILE_PATTERN):
    
    time_init_csvfile = time.time()

    metrics_id_prefix = Path(csvfilename).stem # csvfilename without extension
    
    print "###### " + csvfilename
    with open(csvfilename, 'rb') as csvfile:
        csvreader = csv.reader(skip_comments(csvfile), delimiter=',', quotechar='"')
        csv_list = list(csvreader)
        for row in csv_list:
            print "### " + str(row)
            if len(row) != 3:
                print("file: " + csvfilename + " - incorrect number of columns in row: " + row)
                continue
            root_folder      = row[0]
            try:
                DAYS_to_compress = float(row[1])
                DAYS_to_remove   = float(row[2])
            except Exception as err:
                print('Error ' + str(err) + ' WHEN attempting to extract float numbers from HK rule. This line will be skipped: )')
                
            other_more_specific_root_folders_list = [row[0] for row in all_policy_entries if row[0] != root_folder and row[0].startswith(root_folder)]
            # print ('other_more_specific_root_folders_list: ' + str(other_more_specific_root_folders_list))

            Housekeep(metrics_id_prefix, root_folder, DAYS_to_compress, DAYS_to_remove, other_more_specific_root_folders_list)
    
    time_end_csvfile = time.time()    
    print("File " + csvfilename + " - Elapsed time in secconds: " + str(time_end_csvfile - time_init_csvfile))
    metrics_add(metrics_id_prefix + '._elapsedTimeAggregated_All',  str(time_end_csvfile - time_init_csvfile))
    
    metrics_send_pickle()


time_end_global = time.time()

print("Global Elapsed time in secconds: " + str(time_end_global - time_init_global))

metrics_conn_close()
