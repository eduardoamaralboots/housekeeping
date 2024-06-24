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


def metrics_conn_open():
    # Placeholder for opening a connection to metrics service
    pass


def metrics_conn_close():
    # Placeholder for closing a connection to metrics service
    pass


def metrics_add(metric_name, value):
    # Placeholder for adding a metric
    print(f"Metric added: {metric_name} = {value}")


def metrics_add_folderinfo(metric_name, length, total_size, max_size, min_size, avg_size, sd_size):
    # Placeholder for adding folder info metrics
    print(f"Folder metrics added: {metric_name} = len: {length}, total: {total_size}, max: {max_size}, min: {min_size}, avg: {avg_size}, sd: {sd_size}")


def metrics_send_pickle():
    # Placeholder for sending metrics
    pass


def escape_metricname(name):
    return name.replace('/', '_').replace(' ', '_')


def get_filesizeslist(file_list):
    return [file.getsize() for file in file_list]


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
