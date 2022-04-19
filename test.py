from json import loads
import pandas as pd
import concurrent.futures
import threading


mainMutex  = threading.Lock()

# main=pd.DataFrame()


#Function to return file as lines
def read_file(file_name):
    with open(file_name,encoding='utf-8') as f:
        return f.readlines()

#Looping through files
def extract(month):
    print(f'Extraction of file {month} has started')
    #Initializing primary DataFrame
    #global main
    path=f"mongo-gplay-{month}.json"

    lines=[]
    try:
        with open(path,encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f'failed to read file {path} for {month}: {e}')
        return
        
    
    print(f'Loaded file for {month}')
    #Looping through lines
    mainMutex.acquire()
    print(f'acquired lock for {month}')
    try:
        for line in lines:
            
            #Convert dictionary to DataFrame object and add to primary DataFrame (main)
            # mainMutex.acquire()
            global main
            main = pd.concat([main,pd.json_normalize(loads(line),sep='_')],axis=0,ignore_index=True)
            main = main.drop_duplicates(subset=['appId','appData_genre'],keep='first',ignore_index=True)
    finally:
        mainMutex.release()
    print(f'Extraction of file {month} has ended')
    # return(main)
    
# if __name__ == '__main__':
def set_up_threads(months):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        output = executor.map(extract,months)
        executor.shutdown(wait=True)
    

#Initializing primary DataFrame
if __name__ == "__main__":
    
    main = pd.DataFrame()
    months=['2017-01','2017-02']

    set_up_threads(months)
    #Merge the genre and object IDs for apps with dissimilar genres
    main['appData_genre'] = main.groupby(['appId'])['appData_genre'].transform(lambda x : ','.join(x))
    main['_id_$oid'] = main.groupby(['appId'])['_id_$oid'].transform(lambda x : ','.join(x))
    main['scrapTime_$numberDouble'] = main.groupby(['appId'])['scrapTime_$numberDouble'].transform(lambda x : ','.join(x))
    main = main.drop_duplicates(subset=['appId','appData_genre'],keep='first',ignore_index=True)
    #Import DataFrame to JSON file
    result = main.to_json("Result1.json",orient="records",lines=True)


