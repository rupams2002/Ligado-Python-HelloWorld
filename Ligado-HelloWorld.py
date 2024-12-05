#!/usr/bin/python3
#********************************************************************
# DEVELOPER    : MOHAMMED HASAN
# DATE         : 28 NOVEMBER 2024
# DESCRIPTION  : THIS APP WILL FEED GPS REPORT TO MYSQL
# VERSION      : 1.0.0
#********************************************************************

import time
import json
import mysql.connector
#import splunklib.client as client
#import splunklib.results as results



def SplunkConnect():
    # Connect to Splunk (My Laptop)
    #splunk_connection = client.connect(host="10.10.70.167", username="mohammed", password="Rupam**123")
    # Connect to Splunk (DEV)
    #splunk_connection = client.connect(host="10.5.18.183", username="mohammed", password="Rupam**123")
    # Connect to Splunk (PROD)
    splunk_connection = client.connect(host="10.5.16.240", username="mohammed", password="Rupam**123")

    return splunk_connection


def SplunkSearch(splunk_connection):
    # Search query
    search_query = "search sourcetype=awslog \"report/gps\" earliest=-15m"
    
    # Get search results
    job = splunk_connection.jobs.create(search_query)

    while not job.is_done():
        time.sleep(1)  # wait for the job to complete

    job_results = job.results()
    return job_results



def SplunkData(job_results):
    data_list = []
    # Process results
    reader = results.ResultsReader(job_results)
    for result in reader:
        # print(result)

        _raw = result.get('_raw')
        if _raw:
            try:
                data = json.loads(_raw)
                awslog = json.loads(data['awslog'])
                log_events = awslog.get('logEvents', [])

                for event in log_events:
                    message = event.get('message')
                    if message and "gdm_request" in message:
                        # print(message)

                        # Extract JSON object from the message string
                        json_start = message.index('{')
                        json_str = message[json_start:]
                        json_str = json_str.replace("'", '"')  # Replace single quotes with double quotes for JSON

                        gdm_request = json.loads(json_str)['gdm_request']
                        # print(f"Age: {gdm_request.get('age')}")
                        # print(f"Beam ID: {gdm_request.get('beamId')}")
                        # print(f"GPS Time: {gdm_request.get('gpsTime')}")
                        # print(f"Latitude: {gdm_request.get('latitude')}")
                        # print(f"Longitude: {gdm_request.get('longitude')}")
                        # print(f"Report Type: {gdm_request.get('reportType')}")
                        # print(f"RTIN: {gdm_request.get('rtin')}")
                        # print("\n")

                        # Create a dictionary for each gdm_request
                        request_data = {
                            "Age": gdm_request.get('age'),
                            "Beam ID": gdm_request.get('beamId'),
                            "GPS Time": gdm_request.get('gpsTime'),
                            "Latitude": gdm_request.get('latitude'),
                            "Longitude": gdm_request.get('longitude'),
                            "Report Type": "VOICE" if gdm_request.get('reportType') == 2 else "PTT" if gdm_request.get('reportType') == 3 else gdm_request.get('reportType'),
                            "RTIN": gdm_request.get('rtin')
                        }
                        
                        # Append the dictionary to the data list
                        if any(value is not None for value in request_data.values()):
                            data_list.append(request_data)

            except json.JSONDecodeError as e:
                print(f"JSONDecodeError: {e}")
                print(f"Invalid JSON content: {_raw}")
            except Exception as e:
                print(f"Error processing message: {e}")

    return data_list



def Insert_Into_DB(data_list):
    # Connect to MySQL database
    connection = mysql.connector.connect(
        # host="10.10.130.172",
        host="10.10.70.167",
        user="root",
        password="secret",
        database="gps_report"
    )
    cursor = connection.cursor()

    # SQL insert statement
    insert_query = """
    INSERT INTO gps_report (Age, BeamID, GPSTime, Latitude, Longitude, ReportType, RTIN)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # Iterate over data_list and insert each item into the database
    for data in data_list:
        cursor.execute(insert_query, (
            data["Age"],
            data["Beam ID"],
            data["GPS Time"],
            data["Latitude"],
            data["Longitude"],
            data["Report Type"],
            data["RTIN"]
        ))

    # Commit the transaction
    connection.commit()

    # Close the cursor and connection
    cursor.close()
    connection.close()


#**********************************************************************
# START MAIN
#**********************************************************************

# splunk_connection = SplunkConnect()
# job_results = SplunkSearch(splunk_connection)
# data_list = SplunkData(job_results)
# Insert_Into_DB(data_list)

while True: 
    print("Welcome to Ligado - Python Application Version 1.0.0\n")    
    time.sleep(60)
