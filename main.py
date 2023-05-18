# [START storage_download_file]
from google.cloud import storage
import pandas as pd
import datetime as dt
import requests, time
from requests_ip_rotator import ApiGateway
from bs4 import BeautifulSoup


def download_blob(site):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket('holland_bkp')


    blob = bucket.blob('{}.txt'.format(site))
    blob.download_to_filename(site)

def to_date_obj(timestamp):
    return dt.datetime.strptime(timestamp[:14], '%Y%m%d%H%M%S')



non_html = ('.aac', '.abw', '.arc', '.avif', '.avi',
 '.azw', '.bin', '.bmp', '.bz', '.bz2', '.cda',
  '.csh', '.css', '.csv', '.doc', '.docx', '.eot', 
  '.epub', '.gz', '.gif', '.ico', '.ics', '.jar',
   '.jpeg', '.jpg', '.js', '.jsonld', '.mid', '.midi', 
   '.mjs', '.mp3', '.mp4', '.mpeg', '.mpkg', '.odp', '.ods',
    '.odt', '.oga', '.ogv', '.ogx', '.opus', '.otf', '.png', 
    '.tar', '.tif', '.tiff', '.ts', '.ttf', '.vsd', '.wav', 
    '.weba', '.webm', '.webp', '.woff', '.woff2', '.xls', 
    '.xlsx', '.xul', '.zip', '.3gp', '.3g2', '.7z')


def tranform(file):


    try:

        df = pd.read_csv(file, header=None, delim_whitespace=True)
        df.columns = ["urlkey", "timestamp", "original",
                      "mimetype", "statuscode", "digest", "length"]
        df = df.astype({'timestamp': 'string'})

        df['date'] = df['timestamp'].apply(lambda x: to_date_obj(x))
        df['quarter'] = df['date'].dt.to_period('Q')
        df = df.loc[df.groupby(['quarter', 'urlkey'])['date'].idxmax()]
        df=df[~df.original.str.endswith(non_html)]
        df.drop(columns =['mimetype', 'statuscode', 'length'], inplace=True)

      
        df['raw_url'] = "https://web.archive.org/web/" + \
            df['timestamp']+"id_/"+df['original']
            
        return df

    
    except Exception as e:
        print(e)

def initaite_gateway():
    
    g= ApiGateway( site="http://web.archive.org",
                access_key_id= "AKIAZ77KOHU3FBNF2XT7",
                access_key_secret=  "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"
                ) 
    session = requests.Session()
    session.mount("http://web.archive.org", g)
    return session


def populate(raw_url):
    global session


    try:    
        response = session.get(raw_url)
        if response.status_code ==200:
                
            soup = BeautifulSoup(response.content, features="lxml")
            body = soup.find('body').text
            return(body)
        else:
            return '404'
        #print(response.status_code)
    except:
        return '404'

if __name__ == "__main__":
    sites_file=open('sites.txt', 'r')
    files=sites_file.read().split()
    
    for file in files:
  
            
        session= initaite_gateway()
        # file='101-commerce.com'
        download_blob(file) #saves file from bucket
        # print('file downloaded...')
        df=tranform(file)  # converts file to df
        
        #df.to_csv('trans_df.csv')
        # print('{} transformed to df'.format(file))
        # start=time.time()
        # #df['content']= df['raw_url'].apply(populate)
        df['content'] = df['raw_url'].apply(populate)

    
        # print('{} populated...'.format(file))
        # end = time.time()
        # total_time = end - start
        # print(total_time)
        
        df.to_csv('{}.csv'.format(file))
        
   
    