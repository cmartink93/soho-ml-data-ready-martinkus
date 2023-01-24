import numpy as np

import os, fnmatch
from os import listdir
from os.path import isfile, join

from datetime import timedelta
from dateutil import parser

from sunpy.time import TimeRange

#from astropy.io.fits import Header

import h5py
import csv
from tqdm import tqdm

import json
import pandas as pd

"""
Encoder to solve TypeError: Object of type 'int64' is not JSON serializable from stackoverflow 
"""
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
        
"""
Small class to hold frequently used parameters and base-specific methods
"""
class Base_Class:
    
    #static attributes
    data_raw_times_num = 3
    
    def __init__(self, base_full, home_dir, time_step, date_start, date_finish):
        self.base_full = base_full
        self.home_dir = home_dir
        self.time_window = time_step
        self.date_start = date_start
        self.date_finish = date_finish
        
        
    #dictionary for various scenarios
    
    """
    Uses base/product input information to assign any necessary properties that will
    be used by future functions.
    """
    def set_base_dictionary(self):
        if 'MDI' in self.base_full:
            self.base = 'MDI'
            self.mission = 'SOHO'
        elif 'HMI' in self.base_full:
            self.base = 'HMI'
            self.mission = 'SDO'
        elif 'AIA' in self.base_full:
            self.base = 'AIA'
            self.wavelen = int(self.base_full[3:])
            self.mission = 'SDO'
        elif 'EIT' in self.base_full:
            self.base = 'EIT'
            self.wavelen = int(self.base_full[3:])
            self.mission = 'SOHO'
            self.data_raw_times_num = 2
        elif 'LASCO' in self.base_full:
            self.base = 'LASCO'
            self.mission = 'SOHO'
            self.detector = self.base_full.split('_')[1]
        else:
            print('Not a valid base name')
            
    
    #class methods
    """
    READS THE TIME WINDOW USED WHEN RUNNING SOHO_DATA_GEN.PY FROM THE H5 DATA FILE. THIS TIME_WINDOW IS TIME_STEP_PREV HERE.
    """
    def time_step_prev_reader(self):
        pattern = f'*{self.base}*{self.mission}*[metadata]*[!sync].h5'
        name = pattern_finder(self.home_dir, pattern)
        print('name from time_step_prev_reader:', name)
        time_step_prev = name.split('_')[-5] #was [-4] previously and [-2] before that
        print('time_step_prev from fcn:', time_step_prev)
        
        return int(time_step_prev)
    
    
    """
    READ IN TIMES FROM FITS FILES PER PRODUCT TYPE IF THESE HAVE BEEN GENERATED LOCALLY BY PREVIOUSLY RUNNING SOHO_DATA_GEN.PY.
    FITS FILES ARE ALL UNIQUE AND SORTED TO ENSURE THAT THEY FOLLOW THE ORDER OF THE H5 DATA CUBE THAT HAS BEEN MADE EARLIER BY RUNNING SOHO_DATA_GEN.PY.
    """
    def fits_times_reader(self): 
    
        print('base:', self.base)
        filepath = self.home_dir + self.base  + f'_{self.mission}' + '/'
    
        data_files_pre_pre = [f for f in listdir(filepath) if isfile(join(filepath, f))]
        data_files_pre = [f for f in data_files_pre_pre if 'fits' in f] #to ensure that only FITS files are collected, just in case    
        data_files = np.sort(data_files_pre)
        
        data_raw_times = [elem.split('_')[self.data_raw_times_num] for elem in data_files]
         
            
        return data_raw_times
    
    """
    READ IN TIMES FROM CSV FILES PER PRODUCT TYPE. TAKING UNIQUE VALUES TO ENSURE UNIQUE TIMES IN CSV FILES. {THIS IS THE CASE EXCEPT FOR LASCO_C2 WHERE THERE APPEARS ONCE TIME DUPLICATE}
    """
    def csv_times_reader(self): 
        
        pattern = f'*{self.base}*{self.mission}*[!sync].csv'
        name = pattern_finder(self.home_dir, pattern)
        print('name from csv_times_reader:', name)
        csv_data = pd.read_csv(name, usecols= ['time_at_ind'])
        csv_uniq_times = list(np.unique(csv_data))
        print('len(csv_uniq_times):', len(csv_uniq_times))
                    
        return csv_uniq_times
    
    """
    FIND CORRESPONDING DATA CUBE PER PRODUCT AND EXTRACT ITS DATA AND DIMENSION.
    """
    def cube_data_reader(self):
        pattern = f'*{self.base}*{self.mission}*[metadata]*[!sync].h5'
        name = pattern_finder(self.home_dir, pattern)            
        print('cube name:', name)
        cube_dim = name.split('_')[-2] #.split('.')[0] #str
        print('cube_dim:', cube_dim)
                    
        cube = h5py.File(f'{self.home_dir}{self.name}', 'r')
        cube_data = cube[f'{self.base}_{self.mission}_{cube_dim}'][:]
        
        meta_items_pre = cube[f'{self.base}_{self.mission}_{cube_dim}_metadata'][()]    
        print('len(meta_items_pre):', len(meta_items_pre))
        
        cube.close()
    
        return cube_data, cube_dim, meta_items_pre ### meta_items #, str(cube_hdr) ???????
    
    """
    OUTPUTS DATA CUBES FOR EACH SPECIFIED PRODUCT. THESE CUBES ARE THE REDUCED VERSIONS OF THE ORIGINAL ONES SINCE ONLY THE TIME SLICES THAT COME WITHIN THE SPECIFIED TIME_STEP HAVE BEEN RETAINED.
    #cube_hdr method previously tried where meta_items replaced by cube_hdr
    """
    
    def cube_sync_maker(self, base_list_len, cube_data, cube_dim, meta_items_pre, ind_start, ind_end, sync_time_inds_mod, time_step_prev, flag_lasco=None):
    
         ### Fetching the metadata from the pre-synced data cubes ###
              
         meta_items = json.loads(meta_items_pre)
         meta_data_keywords_pre = list(meta_items.keys())
         print('len(meta_data_keywords_pre):', len(meta_data_keywords_pre))
         
         metadata_keywords_list = []
         for i,ind in tqdm(enumerate(sync_time_inds_mod)):
              metadata_keywords_pre = list(filter(lambda x: f'_{ind}' in x, meta_data_keywords_pre))
              metadata_keywords = list(filter(lambda x: len(str(ind)) == len(x.split('_')[-1]),metadata_keywords_pre)) ### need this second filter to have exact match
              metadata_keywords_list += metadata_keywords
         print('len(metadata_keywords_list):', len(metadata_keywords_list))
         
                   
         if flag_lasco is None:
              cube_data_mod_pre_pre = cube_data[ind_start:ind_end+1]
              cube_data_mod_pre = np.array([cube_data_mod_pre_pre[i] for i in sync_time_inds_mod])
              cube_data_mod = cube_data_mod_pre.astype('int16')
              file_name = f'{self.home_dir}{self.date_start}_to_{self.date_finish}_{self.base}_{self.mission}_{base_list_len}products_{time_step_prev}_{self.time_step}_{cube_dim}_metadata_sync.h5'

         else:
              cube_data_mod = cube_data.astype('int16')
              file_name = f'{self.home_dir}{self.date_start}_to_{self.date_finish}_{self.base}_{flag_lasco}_{self.mission}_{base_list_len}products_{time_step_prev}_{self.time_step}_{cube_dim}_metadata_sync.h5'
              
         cube_sync = h5py.File(file_name, 'w')
         cube_sync.create_dataset(f'{self.base}_{self.mission}_{cube_dim}', data=cube_data_mod) #not compressing images here since images compressed initially in data generation step #compression="gzip"

              
         ### metadata method continued ###
         slice_val_start = int(metadata_keywords_list[0].split('_')[-1])
         print('slice_val_start:', slice_val_start)
         
         meta_data_dict = {}
         slice_counter = 0
         for i,met in tqdm(enumerate(metadata_keywords_list)):
              if int(met.split('_')[-1]) > slice_val_start:
                   slice_val_start = int(met.split('_')[-1]) #update slice_val_start to the next slice
                   slice_counter +=1 #update to count next slice in sync cube
              meta_data_dict[f'{met}_syncslice{slice_counter}'] = meta_items[met] ### these are the meta_items dictionary values    
              #cube_sync.attrs[f'{met[0]}_syncslice{slice_counter}'] = met[1]
         print('data cube slice count:', slice_counter)
              
         cube_sync.create_dataset(f'{self.base}_{self.mission}_{cube_dim}_metadata', data=json.dumps(meta_data_dict, cls=NpEncoder))
         cube_sync.attrs['NOTE'] = 'JSON serialization'
         
         cube_sync.close()     
         
         return cube_sync
    
    """
    OUTPUTS A CSV FILE CONTAINING THE RETAINED TIMES PER SPECIFIED PRODUCT WHICH COINCIDE WITH THE TIMES OF OTHER PRODUCTS WITHIN THE TIME_STEP.
    """
    def csv_time_sync_writer(self, base_list_len, cube_dim, sync_time_list_mod, time_step_prev, flag_lasco=None):
         if flag_lasco is None:
             file_name = f'{self.home_dir}{self.date_start}_to_{self.date_finish}_{self.base}_{self.mission}_{base_list_len}products_{time_step_prev}_{self.time_step}_{cube_dim}_times_sync.csv'
         else:
             file_name = f'{self.home_dir}{self.date_start}_to_{self.date_finish}_{self.base}_{self.mission}_{base_list_len}products_{flag_lasco}_{time_step_prev}_{self.time_step}_{cube_dim}_times_sync.csv'
         
         if not isfile(file_name):
             with open(file_name, 'a') as f:
                 writer = csv.writer(f, delimiter='\n')
                 writer.writerow(sync_time_list_mod)
    
    
    
    
    
    
    
    
#Mission_Product_Sync.py and BaseClass helper functions   

"""
FIND SPECIFIED PATTERN USING FNMATCH 
"""
def pattern_finder(home_dir, pattern):

    for root, dirs, files in os.walk(home_dir):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                name = str(name)
                break
        break
    
    return name

      
"""
CHECKS THAT THE DIMENSION AMONG THE H5 CUBE AND CSV FILES COMING FROM THE DIFFERENT SPECIFIED PRODUCTS IS INDEED THE SAME.
"""
def dimension_checker_from_h5cube_csv(home_dir, base_list, mission): #assumes all h5 files in the same directory which is one above the product (base) directory

    data_dim_list = []
    for base in base_list:
        base = base.strip(' ')
        
        name = pattern_finder(home_dir, pattern = f'*{base}*{mission}*[metadata]*[!sync].h5')
        print('cube name:', name)
        cube_dim = name.split('_')[-2] #.split('.')[0]
        print('cube_dim:', cube_dim)
        data_dim_list.append(cube_dim)

    ind_dim = np.where(data_dim_list[0] == np.array(data_dim_list))[0] 
     
    if len(ind_dim) == len(data_dim_list):
        return True
    else: 
        return False        

"""
CONVERTS TIME STRINGS INTO DATETIME OBJECTS. ALLOWS TO SPECIFY A SUBSET OF THE DATE RANGES THAT HAD BEEN USED WHEN RUNNING MISSION_DATA_GEN.PY. 
"""
def times_actualizer(data_raw_times, date_start, date_finish): #produces a subset of the original times if so desired by the user. Adds flexibility in choice of time range!

    date_time_pre_start = date_start + '-0000'
    date_time_start= parser.parse(date_time_pre_start)
    print('date_time_start:', date_time_start)

    date_time_pre_end = date_finish + '-2359'
    date_time_end = parser.parse(date_time_pre_end)
    print('date_time_end:', date_time_end)
    
    data_times = np.array([parser.parse(elem) for elem in data_raw_times]) #this needs to be an array for the where fcn right below not to require array explicitely there!
    
    ind_start = np.where(data_times >= date_time_start)[0] #shifted the second [0] below just in case dates were selected incorrectly
    ind_end = np.where(data_times <= date_time_end)[0] #shifted the second [-1] below just in case dates were selected incorrectly
    
    if (len(ind_start) != 0) and (len(ind_end) != 0): #so very important to have -1 and not 0 here since ind_end[-1] corresponds to the last date
        data_times_revised = data_times[ind_start[0]:ind_end[-1]+1] #a flexible range offered here #+1 since bracket range is one less. #this corresponds to cube_data[ind_start:ind_end+1] in cube_sync_maker.
        print('len(data_times_revised):', len(data_times_revised))
    
    else:
        data_times_revised = data_times.copy()
        raise ValueError("date selected is outside original date start and date finish range") 
        
    return list(data_times_revised), data_times, ind_start, ind_end

"""
FINDS THE TIME_WINDOW USED PREVIOUSLY WHEN RUNNING SOHO_DATA_GEN.PY. IN THIS PROGRAM THE NAME IS CHANGED TO TIME_STEP.
"""
def min_time_step(data_times):
    
    data_times_diff_pre = data_times[1:] - data_times[:-1]
    data_times_diff = [elem for elem in data_times_diff_pre] #.total_seconds()
    
    min_time_diff = np.min(data_times_diff)

    return min_time_diff


    

    

"""
START WITH SHORTEST PRODUCT TIME LIST SINCE THAT'S THE NATURAL BOTTELNECK.
"""
def shortest_prod_list_index_finder(product_list):

     product_list_lengths = [len(i) for i in product_list]
     print('product_list_lengths:', product_list_lengths)
     product_list_lengths_min = np.min(product_list_lengths)
     ind_min_len = np.where(np.array(product_list_lengths) == product_list_lengths_min)[0][0]
     print('product_list_lengths[ind_min_len]:', product_list_lengths[ind_min_len])
     
     return ind_min_len


"""
FINDING THE TIMES AND INDICES THAT ARE syncED BETWEEN THE ENTERED PRODUCTS. MOVES ALONG THE SHORTEST PRODUCT LIST. OVERLAP TIME INTERVAL DETERMINED BY MOVING +/- HALF OF THE ORIGINAL TIME_STEP.
THEN IF THE TIME STEP ENTERED IN THIS CODE IS AN INTEGER MULTIPLE OF THE ORIGINAL TIME STEP, THE syncED TIMES COMPUTED ON THE ORIGINAL TIME STEP ARE SUBSEQUENTLY SUBSAMPLED.
"""
def sync_times_and_inds(product_list, ind_min_len, time_step, time_step_prev): #main engine of the algorithm

    sync_time_list = []
    sync_time_inds_list = []
    
    ratio = int(time_step / time_step_prev) #in order to subsample (e.g., time_step_prev=6 but now want time_step=12, so will need to take every other element found from syncing by original time)
    print('ratio:', ratio)
    
    for i,time_val in tqdm(enumerate(product_list[ind_min_len])): #so moving along shortest product list
        time_range = TimeRange(time_val - timedelta(hours=time_step_prev/2), time_val + timedelta(hours=time_step_prev/2)) #heart of the algorithm
        #always sync on original time_window and then can subsample from that if necessary. #time_val is a datetime object so can subtract a timedelta from it.
        temp_ind_list = []
        temp_time_list = []
        for j,product in enumerate(product_list):
            time_range_list = [(item in time_range) for item in product] #boolean #bool
            if any(np.array(time_range_list)): #to ensure that have at least one entry that is True.
                ind_temp = np.where(time_range_list)[0][0] #this second [0] ensures that only the first matching time is taken and hence that the resulting times sieved are uniquely obtained!
                temp_time_pre = product[ind_temp]
                temp_time = ''.join(str(temp_time_pre).split(' ')[0].split('-')) + ''.join(str(temp_time_pre).split(' ')[1].split(':'))
            else:
                ind_temp = np.nan
                temp_time = np.nan
            
            temp_ind_list.append(ind_temp)
            temp_time_list.append(temp_time)
        
        if len(np.where(np.array(temp_ind_list) != np.array(temp_ind_list))[0]) == 0: #this is used to pick only those instances where nan vals are not present.
            sync_time_inds_list.append(temp_ind_list) #is of len(base_list)            
            sync_time_list.append(temp_time_list) #is of len(base_list)     

    return sync_time_inds_list[::ratio], sync_time_list[::ratio]


"""
USE THE TIMES AND INDICES THAT ARE syncED BETWEEN THE ENTERED PRODUCTS AND REORDER BY ORDER OF PRODUCTS FOLLOWING ORDER OF BASE_LIST: USER ENTERED ORDER OF PRODUCTS
"""
def sync_times_and_inds_sort_by_product(sync_time_inds_list, sync_time_list):

     sync_time_inds_list_ravel = np.ravel(sync_time_inds_list, order='F')
     sync_time_inds_list_mod = np.hsplit(sync_time_inds_list_ravel,len(sync_time_inds_list[0])) #len(sync_time_inds_list[0]) should be equal to len(base_list)!
     
     sync_time_list_ravel = np.ravel(sync_time_list, order='F')
     sync_time_list_mod = np.hsplit(sync_time_list_ravel,len(sync_time_list[0])) #len(sync_time_list[0]) should be equal to len(base_list)!
     
     return sync_time_inds_list_mod, sync_time_list_mod

"""
OUTPUTS TIMES AND INDICES TO BE USED FROM LASCO DIFFERENCE IMAGES. ### NEED TO TAKE INTO ACCOUNT THAT COULD HAVE BOTH C2 AND C3 PRESENT SIMULTANEOUSLY!!!
"""
def lasco_diff_times_inds(lasco_sync_times):
     
     #print('lasco_sync_times_internal:', lasco_sync_times)
     synced_lasco_datetimes = [parser.parse(elem) for elem in lasco_sync_times]
     #print('synced_lasco_datetimes:', synced_lasco_datetimes)
     synced_lasco_datetimes_Fcorona_remov_pre = np.array(synced_lasco_datetimes[1:]) - np.array(synced_lasco_datetimes[:-1])
     #print('synced_lasco_datetimes_Fcorona_remov_pre:', synced_lasco_datetimes_Fcorona_remov_pre)
     synced_lasco_datetimes_Fcorona_remov = [np.round(elem.total_seconds()/3600.) for elem in synced_lasco_datetimes_Fcorona_remov_pre]
     #print('synced_lasco_datetimes_Fcorona_remov:', synced_lasco_datetimes_Fcorona_remov)
     lasco_ind_Fcorona_24h = np.where(np.array(synced_lasco_datetimes_Fcorona_remov) <= 24)[0]
     #print('lasco_ind_Fcorona_24h:', lasco_ind_Fcorona_24h)
     print('len(lasco_ind_Fcorona_24h):', len(lasco_ind_Fcorona_24h))     
     
     return lasco_ind_Fcorona_24h


  