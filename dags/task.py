from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  
from pendulum import today
from io import BytesIO

import asyncio
import websockets
import json
from datetime import datetime, timezone
import pandas as pd
import time
import os

csv_path=os.path.join(os.path.dirname(__file__), 'new_ais_position_reports_oil_tankers.csv')
#ais block starts
async def connect_ais_stream_async():
    while True:  # Infinite loop to attempt reconnection
        try:
            print("Connecting to WebSocket...")
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                subscribe_message = {
                    "APIKey": "ed1fd477c0690d1d2333f90cde585489a9956958",
                    "BoundingBoxes": [[[-11, 178], [30, 74]]]
                }
                subscribe_message_json = json.dumps(subscribe_message)
                await websocket.send(subscribe_message_json)
                
                # Dictionary to store data by MMSI
                mmsi_dict = {}

                async for message_json in websocket:
                    message = json.loads(message_json)
                    message_type = message.get("MessageType", None)

                    if message_type == "PositionReport":
                        ais_message = message['Message']['PositionReport']
                        mmsi = ais_message.get("UserID")

                     
                        # Extract required fields
                        required_data = {
                            "MMSI": mmsi,
                            "BaseDateTime": datetime.now(timezone.utc),
                            "SOG": ais_message.get("Sog"),
                            "COG": ais_message.get("Cog"),
                            "LON": ais_message.get("Longitude"),
                            "LAT": ais_message.get("Latitude"),
                            
                        }
                        
                        
                        # Update the dictionary if MMSI already exists
                       
                        mmsi_dict[mmsi] = required_data
                        
                        # Convert the dictionary to DataFrame and save it to CSV
                        df = pd.DataFrame(mmsi_dict.values())
                        # df.to_csv(r'D:\airflow_local\dags\new_ais_position_reports_oil_tankers.csv', index=False)
                        df.to_csv(csv_path, index=False)
                        print(f"Data updated and saved at {datetime.now(timezone.utc)}")

        except (websockets.ConnectionClosedError, websockets.InvalidState):
            print("WebSocket connection lost. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
#ais block ends

def connect_ais_stream():
    asyncio.run(connect_ais_stream_async())

#anomaliees block starts
import pandas as pd
import math
from sklearn.ensemble import IsolationForest as is_model

def haversine_formulae(long1,lat1,long2,lat2):
    R=6371
    long1,lat1,long2,lat2=map(math.radians,[long1,lat1,long2,lat2])
    delta_long=long2-long1
    delta_lat=lat2-lat1
    return 2*R*(math.sin(delta_long/2)**2+math.cos(lat1)*math.cos(lat2)*math.sin(delta_lat/2)**2)

def anomaly():
    # ais_file=pd.read_csv(r'D:\airflow_local\dags\new_ais_position_reports_oil_tankers.csv')
    ais_file=pd.read_csv(csv_path)
    ais_file.isnull().sum()
    ais_file=ais_file.dropna()

    ais_file['csog']=ais_file.groupby(['MMSI'])['SOG'].diff().fillna(0)

    ais_file['ccog']=ais_file.groupby(['MMSI'])['COG'].diff().fillna(0)

    ais_file['prev_long']=ais_file.groupby(['MMSI'])['LON'].shift()

    ais_file['prev_lat']=ais_file.groupby(['MMSI'])['LAT'].shift()

    ais_file['distance']=ais_file.apply(
        lambda x: haversine_formulae(x.prev_long,x.prev_lat,x.LON,x.LAT),axis=1)

    anomaly_model=is_model(contamination=0.01)

    anomaly_model.fit(ais_file[['SOG','COG','csog','ccog','distance']])

    #If it is anomaly then the value is -1 else the value is 1
    ais_file['anomalies']=anomaly_model.predict(ais_file[['SOG','COG','csog','ccog','distance']])

    print("\nAnomaly Counts:\n", ais_file['anomalies'].value_counts())

    #collecting the data for anomaly=-1
    anomalies=ais_file[ais_file['anomalies']==-1]
    return anomalies

#anomalies block ends
import requests
from requests.auth import HTTPBasicAuth

# Your Sentinel Hub credentials
def get_access_token():
    CLIENT_ID = "your_client_id"  # Replace with your actual client ID
    SECRET_KEY = 'your_secret_key'  # Replace with your actual secret key

    API_URL = 'https://services.sentinel-hub.com/oauth/token'
    payload = {'grant_type': 'client_credentials'}

    response = requests.post(API_URL, auth=HTTPBasicAuth(CLIENT_ID, SECRET_KEY), data=payload)

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print('Failed to obtain access token:', response.status_code, response.text)
        return None




import os
import requests
from datetime import datetime, timezone

def image():
    anomalies = anomaly()  # Get anomaly DataFrame

    delta = 0.1
    access_token = os.getenv("SENTINEL_HUB_ACCESS_TOKEN", get_access_token())

    url = "https://services.sentinel-hub.com/api/v1/process"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    for _, row in anomalies.iterrows():
        lon_min = row['LON'] - delta / 2
        lon_max = row['LON'] + delta / 2
        lat_min = row['LAT'] - delta / 2
        lat_max = row['LAT'] + delta / 2

        payload = {
            "input": {
                "bounds": {
                    "bbox": [lon_min, lat_min, lon_max, lat_max],
                    "properties": {"crs": "http://www.opengis.net/def/crs/EPSG/0/4326"}
                },
                "data": [{
                    "type": "sentinel-1-grd",
                    "dataFilter": {
                        "timeRange": {
                            "from": "2017-11-15T00:00:00Z",
                            "to": "2017-11-15T23:00:00Z"
                        },
                        "acquisitionMode": "IW",
                        "polarization": "DV",
                        "orbitDirection": "ASCENDING"
                    },
                    "processing": {
                        "backCoeff": "GAMMA0_ELLIPSOID",
                        "orthorectify": True
                    }
                }]
            },
            "output": {
                "width": 2056,
                "height": 2056,
                "responses": [{
                    "identifier": "default",
                    "format": {"type": "image/png"}
                }]
            },
            "evalscript": """
            //VERSION=3
            function setup() {
                return {
                    input: ["VV", "VH"],
                    output: { bands: 1 }
                };
            }

            function evaluatePixel(sample) {
                var threshold = 0.05;
                return [sample.VV < threshold ? 1 : 0];
            }
            """
        }

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            # Clean timestamp for Windows compatibility
            timestamp = datetime.now(timezone.utc).isoformat().replace(":", "-")
            filepath = os.path.join(os.path.dirname(__file__), 'images', f"oil_spill_{row['MMSI']}_{timestamp}.png")
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"Image saved to {filepath}")
        else:
            print(f"Failed to retrieve image for MMSI {row['MMSI']}: {response.status_code} - {response.text}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
import pendulum
with DAG(
    dag_id='satellite_analysis_pipeline',
    default_args=default_args,
    description='Pipeline for satellite analysis tasks',
    schedule=None, # 1 * * * * 
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    tags=['satellite', 'analysis'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Task for AIS analysis
    task_ais = PythonOperator(
        task_id='ais_analysis',
        python_callable=connect_ais_stream,
    )

    # Task for anomaly detection
    task_anomalies = PythonOperator(
        task_id='anomaly_detection',
        python_callable=anomaly,
    )

    # Task for downloading data (with a timeout)
    task_download = PythonOperator(
        task_id='download_data',
        python_callable=get_access_token,
    )

    # Task for image processing
    task_image = PythonOperator(
        task_id='image_processing',
        python_callable=image,
    )

    # DAG flow
    start >> task_ais >> task_anomalies >> task_download >> task_image >> end
