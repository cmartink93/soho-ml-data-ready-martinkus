from tqdm import tqdm
from time import process_time 
import numpy as np
import Mission_utility.product_time_sync as pts

date_start =  '2010-10-01'#'2002-04-01' #'1999-04-04' # '2010-10-01'
date_finish = '2010-10-05'#'2002-04-07' #'1999-04-09' # '2010-10-05'
time_step = 12
home_dir = '/Users/gohawks/Desktop/soho-ml-data/soho-ml-data-ready-martinkus/'
bases = 'MDI_96m' #,LASCO_C3'#MDI_96m #AIA #HMI #'EIT195'
option = 'Y' #are fits files provided?

#def main(date_start, date_finish, time_step, home_dir, bases, option): #with date_start, date_finish can further regulate which fits files want to start/end with in case different from original prior choice!

start_process_time = process_time() #initialize clock

product_list = [] #list for all products #will need to hsplit this by base_list_len - won't result in equal division: need another approach
slice_start_ind_list = [] #to keep track of the corresponding subset of cube slice indices if the original corresponding time range has been modified.
slice_end_ind_list = []
BaseClass_list = []

base_list = bases.split(',')
base_list_len = len(base_list)
print('base_list:', base_list)
print('time_step in hours:', time_step)
time_step_sec = time_step*3600
    
for base in base_list:
    base = base.strip(' ')
    
    base = base.strip(' ')
    if ('LASCO' in base) or ('EIT' in base):
        BaseClass = pts.Base_Class(base, home_dir, time_step)
        
    else:
        BaseClass = pts.Base_Class(base, home_dir, time_step)
        
    BaseClass.set_base_dictionary()
    BaseClass_list.append(BaseClass)
    
    time_step_prev = BaseClass.time_step_prev_reader()
    print('time_step_prev:', time_step_prev)
    
    if (option == 'Y') or (option == 'y'): #so deal with .fits files since contain all info that need such as the time and dim. additionally have h5 cube and csv file too.
        found_times = BaseClass.fits_times_reader() #based on indiv base!
        dim_err = "there is a mismatch in dimensionality among one or more products (e.g., 128x128 vs 256x256) in the directory"
        
    elif (option == 'N') or (option == 'n'): #so dealing with the downloaded datasync_times_and_inds cubes and csv files but no fits files. so need times from csv and dim from h5 cube or csv. both h5 and csv used.
        found_times = BaseClass.csv_times_reader()
        dim_err = "there is a mismatch in dimensionality among one or more products reported in the h5 and csv files in the directory (e.g., 128x128 vs 256x256)"
        
    product_list.append(pts.times_actualizer(found_times, date_start, date_finish)[0]) #so the first return of times_actualizer which now returns two objects
    slice_start_ind_list.append(pts.times_actualizer(found_times, date_start, date_finish)[2][0]) #first start entry
    slice_end_ind_list.append(pts.times_actualizer(found_times, date_start, date_finish)[3][-1]) #last finish entry
                    
    min_time_diff = pts.min_time_step(pts.times_actualizer(found_times, date_start, date_finish)[1])
    print('min_time_diff in hours:', min_time_diff)
    if time_step_prev > time_step: 
        #so times_actualizer()[1] because want the data_times original and not the revised ones in order to have full range when take min.
        raise ValueError("selected time step has to be greater than or equal to the original time step used (e.g., can not choose < 6 hrs if original time step was 6 hrs)")
        
    elif not pts.dimension_checker_from_fits(home_dir, base_list, BaseClass.mission): #strictly the for loop is about a single base but here already raise exception for all bases because need len(base_list) here!
        raise ValueError(dim_err)
        
    else:
        continue


ind_min_len = pts.shortest_prod_list_index_finder(product_list)
    
synch_time_inds_list, synch_time_list = pts.sync_times_and_inds(product_list, ind_min_len, time_step, time_step_prev)
synch_time_inds_list_mod, synch_time_list_mod = pts.sync_times_and_inds_sort_by_product(synch_time_inds_list, synch_time_list)
#print('synch_time_list_mod_main:', synch_time_list_mod)


lasco_diff_ind_Fcorona_24h_list = []
lasco_diff_len_ind_Fcorona_24h_list = [] #C2 and C3 list should be the same size even after 24 hr index but good to check just in case

for i,BaseClass in tqdm(enumerate(BaseClass_list)):
    #base = base.strip(' ')
    cube_data, cube_dim, meta_items  = BaseClass.cube_data_reader() #, cube_hdr #, meta_items
    BaseClass.cube_sync_maker(base_list_len, cube_data, cube_dim, meta_items, slice_start_ind_list[i], slice_end_ind_list[i], synch_time_inds_list_mod[i], date_start, date_finish, time_step_prev) ###cube_dim, cube_hdr, ##### cube_dim, meta_items
    BaseClass.csv_time_sync_writer(base_list_len, date_start, date_finish, cube_dim, synch_time_list_mod[i], time_step_prev)
    
    if 'LASCO' in BaseClass.base_full:
        
        lasco_ind_Fcorona_24h = pts.lasco_diff_times_inds(synch_time_list_mod[i])
        #print('lasco_ind_Fcorona_24h:',lasco_ind_Fcorona_24h)
        
        if len(lasco_ind_Fcorona_24h)>0:
            lasco_diff_ind_Fcorona_24h_list.append(lasco_ind_Fcorona_24h)
            lasco_diff_len_ind_Fcorona_24h_list.append(len(lasco_ind_Fcorona_24h))
        
#print('lasco_diff_ind_Fcorona_24h_list:', lasco_diff_ind_Fcorona_24h_list)
print('lasco_diff_len_ind_Fcorona_24h_list:', lasco_diff_len_ind_Fcorona_24h_list)
    
if (len(lasco_diff_ind_Fcorona_24h_list)!=0):
    
    flag_lasco =  'Fcorona'   
    
    if (lasco_diff_len_ind_Fcorona_24h_list) and (len(np.unique(lasco_diff_len_ind_Fcorona_24h_list)) > 1):
        ind_lasco_len_Fcorona_24h_min = np.where(np.array(lasco_diff_len_ind_Fcorona_24h_list) == np.min(lasco_diff_len_ind_Fcorona_24h_list))[0][0]
        print('ind_lasco_len_Fcorona_24h_min:', ind_lasco_len_Fcorona_24h_min)
        ind_lasco_principal_Fcorona_24h = lasco_diff_ind_Fcorona_24h_list[ind_lasco_len_Fcorona_24h_min]        
    
    elif (lasco_diff_len_ind_Fcorona_24h_list) and (len(np.unique(lasco_diff_len_ind_Fcorona_24h_list)) == 1):
        ind_lasco_len_Fcorona_24h_min = 0
        ind_lasco_principal_Fcorona_24h = lasco_diff_ind_Fcorona_24h_list[ind_lasco_len_Fcorona_24h_min]
    
    else:
        ind_lasco_principal_Fcorona_24h = 0
        
    for i,BaseClass in tqdm(enumerate(BaseClass_list)): #loop to make time_diff set
        
        cube_data_pre, cube_dim, meta_items = BaseClass.cube_data_reader() #, cube_hdr #, meta_items
        
        if ('LASCO' in base):
            cube_data_diff = cube_data_pre[1:] - cube_data_pre[:-1]
            cube_data = cube_data_diff[ind_lasco_principal_Fcorona_24h] #don't need the +1 here as for the times since this is the difference cube
        
        else:
            cube_data = cube_data_pre[ind_lasco_principal_Fcorona_24h+1]           
        
        BaseClass.cube_sync_maker(base_list_len, cube_data, cube_dim, meta_items, slice_start_ind_list[i], slice_end_ind_list[i], synch_time_inds_list_mod[i][ind_lasco_principal_Fcorona_24h+1], date_start, date_finish, time_step_prev, flag_lasco) #adding one to get to original time value since was taking differce of the time array ###cube_dim, cube_hdr, #####cube_dim, meta_items
        BaseClass.csv_time_sync_writer(base_list_len, date_start, date_finish, cube_dim, synch_time_list_mod[i][ind_lasco_principal_Fcorona_24h+1], time_step_prev, flag_lasco)
        #adding one to get to original time value since was taking differce of the time array
        


end_process_time = process_time()
time_of_process = end_process_time - start_process_time
print('time to run in seconds:', time_of_process)
    


# if __name__ == '__main__':
#     import argparse
#     parser_args = argparse.ArgumentParser(description='Mission Products Synchronization')
#     parser_args.add_argument('--date_start', metavar='time', required=True, help='yyyy-mm-dd, 1996-01-01 is earliest start', type = str)
#     parser_args.add_argument('--date_finish', metavar='time', required=True, help='yyyy-mm-dd, 2011-05-01 is recommended latest finish for SOHO mission', type = str)
#     parser_args.add_argument('--time_step', metavar='time', required=True, help='int, time step in hours', type = int) #was previously time_window
#     parser_args.add_argument('--home_dir', metavar='home directory', required=True, help='str, e.g., "/home/user/Documents/", need "/" in the end', type = str)
#     parser_args.add_argument('--option', metavar='*.fits files present?', required=True, help='str, Y/N or y/n ', type = str)    
#     parser_args.add_argument('--products', metavar='products to sync', required=True, help='str, Enter all the following or a subset thereof, in any order, seperated by commas: "EIT195, MDI_96m, LASCO_C2, LASCO_C3, EIT171, EIT304, EIT284"', type = str) 
    
#     args = parser_args.parse_args()
#     main(
#         date_start = args.date_start,
#         date_finish = args.date_finish,
#         time_step = args.time_step,
#         home_dir = args.home_dir,
#         option = args.option,
#         bases = args.products)
         
