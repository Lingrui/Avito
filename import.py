#!/usr/bin/env python3
import picpac
import pandas as pd 
import sys
import glob
import os
import pickle 

input_file = '/shared/s2/users/lcai/Avito/data/train.csv'
fig_path = '/shared/s2/users/lcai/Avito/data/data/competition_files/train_jpg/'
chunksize = 10 ** 6
val_array = []
#def import_db (Set):
def import_db ():
    i = 0
    train_db = picpac.Writer('/data/scratch/lcai/Avito/db/train.db', picpac.OVERWRITE)
    val_db = picpac.Writer('/data/scratch/lcai/Avito/db/val.db', picpac.OVERWRITE)
    for chunk in pd.read_csv(input_file,chunksize=chunksize):
    #with open('%s.list' % Set, 'r') as f:
        for _,row in chunk.iterrows():
            itemid = row.iloc[0]
            fig = str(row.iloc[15])
            label = row.iloc[17]
            error = 0 
            if os.path.isfile(fig_path + fig + ".jpg"):
                file_path = fig_path + fig + ".jpg"
            elif os.path.isfile(fig_path + fig):
                file_path = fig_path + fig
            else:
                print ("Error!\t",fig,"\t",itemid)
                error = 1
            
            if error != 1:
                with open(file_path, 'rb') as f2:
                    buf = f2.read()
                    if i % 20 == 0: 
                        val_db.append(float(label), buf)
                        val_array.append(itemid)
                        #print ("val",i)
                    else:
                        train_db.append(float(label), buf)
                        #print ("train",i)
            '''
            path = fig_path + fig + '*'
            file_path = glob.glob(path) 
            if len(file_path) != 1:
                print ("Error!!")
                print (file_path)
            else:
                with open(glob.glob(path)[0], 'rb') as f2:
                    buf = f2.read()
                    if i % 20 == 0: 
                        val_db.append(float(label), buf)
                        print ("val",i)
                    else:
                        train_db.append(float(label), buf)
                        print ("train",i)
            '''
            i += 1
            pass
        pass
    del db
    pickle.dump(val_array, open('/data/scratch/lcai/Avito/db/val_id.pkl', 'wb'))
    pass

#import_db('train')
import_db()
