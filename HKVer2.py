import sys
sys.path.insert(0,'./lib')

import time
import itertools
import re
import glob
import csv
import os
from path import Path


def skip_comments(lines):
    comment_pattern = re.compile(r'#.*$')
    for line in lines:
        line = re.sub(comment_pattern, '', line).strip()
        if line:
            yield line

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


def metrics_add(metricname, metricvalue):
    global timestamp
    global pickle_list
    
    pickle_list.append((metricname, (int(timestamp), metricvalue)))
    
    if VERBOSE_LOG_METRICS:
        print(str(metricname) 
              + ',' + metricvalue)

    # Placeholder for adding a metric
    print(f"Metric added: {metricname} = {metricvalue}")


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


def escape_metricname(string):
    string = string.replace('\\', '_')
    string = string.replace(':', '_')
    string = string.replace('/', '_')
    string = string.replace(' ', '_')

    return string


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
  


def metrics_calc_from_filesizeslist(sizes_list):
    if not sizes_list:
        return 0, 0, 0, 0, 0, 0
    length = len(sizes_list)
    total_size = sum(sizes_list)
    max_size = max(sizes_list)
    min_size = min(sizes_list)
    avg_size = total_size / length
    # Standard deviation calculation
    variance = sum((x - avg_size) ** 2 for x in sizes_list) / length
    sd_size = variance ** 0.5
    return length, total_size, max_size, min_size, avg_size, sd_size


def Housekeep(metrics_id_prefix, root_folder, DAYS_to_compress, DAYS_to_remove, other_more_specific_root_folders_list):
    if not Path(root_folder).exists():
        print("root folder " + root_folder + " doesn't exist: nothing to do here")
        return 

    global timestamp
    
    ARCHIVE_FILENAME_PREFIX = 'Archive_'
    ARCHIVE_FILENAME_SUFFIX = '.tar.gz'
    
    time_init_folder = time.time()
    
    dirs = {}

    DAYS_to_remove_tgz = DAYS_to_remove - DAYS_to_compress
    if DAYS_to_remove_tgz < 0:
        print("This is very strange: DAYS_to_remove < DAYS_to_compress. DAYS_to_compress will be useless")
        DAYS_to_remove_tgz = 0
    
    time_in_secs_to_compress = timestamp - (DAYS_to_compress * 24 * 60 * 60)
    time_in_secs_to_remove = timestamp - (DAYS_to_remove * 24 * 60 * 60)
    time_in_secs_to_remove_tgz = timestamp - (DAYS_to_remove_tgz * 24 * 60 * 60)

    time_init_folder_collect = time.time()
    
    root_folder_path = Path(root_folder)
    
    for d in itertools.chain([root_folder_path], root_folder_path.walkdirs()):
        nothing_to_do = False
        for o in other_more_specific_root_folders_list:
            if str(d).startswith(o):
                nothing_to_do = True
                break
        if nothing_to_do:
            print(f"Nothing to do for [{str(d)}] under [{root_folder}] because it is defined more specifically under its own entry in the policy")
            continue
            
        time_init_subfolder_collect = time.time()
        
        if str(d) not in dirs:
            dirs[str(d)] = {'C': [], 'R': [], 'O': [] }

        for i in d.files():
            try:
                if str(i.basename()).startswith(ARCHIVE_FILENAME_PREFIX) and str(i.basename()).endswith(ARCHIVE_FILENAME_SUFFIX):
                    if i.mtime <= time_in_secs_to_remove_tgz:
                        dirs[str(i.dirname())]['R'].append(i)
                elif i.mtime <= time_in_secs_to_remove:
                    dirs[str(i.dirname())]['R'].append(i)
                elif i.mtime <= time_in_secs_to_compress:
                    dirs[str(i.dirname())]['C'].append(i)
                else:
                    dirs[str(i.dirname())]['O'].append(i)
            except Exception as err:
                print(f'Error {str(err)} WHEN collecting info from file: {str(i)}. Skipped')
        
        time_end_subfolder_collect = time.time()
        metrics_add(metrics_id_prefix + '.' + escape_metricname(d) + '._elapsedTime_Collecting', str(time_end_subfolder_collect - time_init_subfolder_collect))

    time_end_folder_collect = time.time()
    metrics_add(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._elapsedTimeAggregated_Collecting', str(time_end_folder_collect - time_init_folder_collect))

    time_init_folder_metrics_calculation = time.time()
    
    sizes_root_allC, sizes_root_allR, sizes_root_allO = [], [], []
    
    for dirname in dirs.keys():
        time_init_subfolder_metrics_calculation = time.time()
                
        metricvalue_size_listC = get_filesizeslist(dirs[dirname]['C'])
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listC)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._toCompress', _len, _sum, _max, _min, _avg, _sd)
    
        metricvalue_size_listR = get_filesizeslist(dirs[dirname]['R'])
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listR)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._toRemove', _len, _sum, _max, _min, _avg, _sd)
    
        metricvalue_size_listO = get_filesizeslist(dirs[dirname]['O'])
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listO)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._others', _len, _sum, _max, _min, _avg, _sd)
        
        _len, _sum, _max, _min, _avg, _sd = metrics_calc_from_filesizeslist(metricvalue_size_listC + metricvalue_size_listR + metricvalue_size_listO)
        metrics_add_folderinfo(metrics_id_prefix + '.' + escape_metricname(dirname) + '._all', _len, _sum, _max, _min, _avg, _sd)
        
        sizes_root_allC += metricvalue_size_listC
        sizes_root_allR += metricvalue_size_listR
        sizes_root_allO += metricvalue_size_listO
        
        time_end_subfolder_metrics_calculation = time.time()
        metrics_add(metrics_id_prefix + '.' + escape_metricname(dirname) + '._elapsedTime_MetricsCalculation', str(time_end_subfolder_metrics_calculation - time_init_subfolder_metrics_calculation))

        metrics_send_pickle()
    
    time_end_folder_metrics_calculation = time.time()
    metrics_add(metrics_id_prefix + '.' + escape_metricname(root_folder) + '._elapsedTimeAggregated_MetricsCalculation', str(time_end_folder_metrics_calculation - time_init_folder_metrics_calculation))

    # Continue with the processing phase and other operations


if __name__ == '__main__':
    POLICY_SUBFOLDER = '/product/softwareag/webmdep/HK/fileBusinessData/policyFiles/ScheduledDailyAt0030'
    POLICY_FILE_PATTERN = 'hk.*.csv'
    
    time_init_global = time.time()
    
    timestamp = time_init_global  
    
    if len(sys.argv) != 2:
        sys.exit(2)
    
    policyFolder = Path(sys.argv[1]) 
    
    if not policyFolder.exists():
        print('Policy folder ' + str(policyFolder) + ' does not exist')
        sys.exit(1)
    
    scriptFolder = Path(__file__).dirname()
    policyFolderRoot = scriptFolder / POLICY_SUBFOLDER
    
    if not policyFolderRoot.exists():
        print('Policy folder root ' + str(policyFolderRoot) + ' does not exist')
        sys.exit(1)
    
    if not policyFolder.startswith(policyFolderRoot):
        print('Policy folder ' + str(policyFolder) + ' is not under Policy folder root ' + str(policyFolderRoot))
        sys.exit(1)
    
    all_policy_entries = []
    
    for p in policyFolderRoot.walk(POLICY_FILE_PATTERN):
        with open(p, 'r') as csvfile:
            csvreader = csv.reader(skip_comments(csvfile), delimiter=',', quotechar='"')
            all_policy_entries += list(csvreader)

    metrics_conn_open()
    
    os.chdir(policyFolder)
    
    for csvfilename in glob.glob(POLICY_FILE_PATTERN):
        time_init_csvfile = time.time()
    
        metrics_id_prefix = Path(csvfilename).stem
        
        print("###### " + csvfilename)
        with open(csvfilename, 'r') as csvfile:
            csvreader = csv.reader(skip_comments(csvfile), delimiter=',', quotechar='"')
            csv_list = list(csvreader)
            for row in csv_list:
                print("### " + str(row))
                if row[0].startswith('#'):
                    continue
                if len(row) != 4:
                    continue
                Housekeep(metrics_id_prefix, str(row[0]), int(row[1]), int(row[2]), list(filter(bool, map(str.strip, row[3].split(';')))))
    
        time_end_csvfile = time.time()
        metrics_add(metrics_id_prefix + '._elapsedTime', str(time_end_csvfile - time_init_csvfile))
    
    metrics_send_pickle()
    metrics_conn_close()
    
    time_end_global = time.time()
    metrics_add('elapsedTime', str(time_end_global - time_init_global))
    
    sys.exit(0)
