import os
import sys
import json
from collections import defaultdict
from pyais import decode
from pyais.stream import FileReaderStream
from tqdm import tqdm
import regex as re


def decode_given_file(file, total_dictionary, num_messages):
    """
    """

    for msg in tqdm(FileReaderStream(file), total=447869):
        try:
            # Decode the message
            decoded_message = msg.decode().asdict()
                        
            # Debug
            #if decoded_message["msg_type"] == 1:
            #    print("\n", decoded_message, "\n")
            #
            #for j in range(0, 27):
            #    if decoded_message["msg_type"] == j+1:
            #        msg_type_count[j] += 1

            # Get the timestamp (year, month, day and hour) from the file's name
            fileName = re.search(r'([^\\]+$)', file)

            date = re.search(r'(?<=\_)([0-9]{2,}-[0-9]{1,}-[0-9]{1,})(?=\_)', str(fileName))
            year = date[0][0:4]
            month = date[0][5:7]
            day = date[0][8:10]
            hour =  file[-7:-5]
                        
            # Write messages to dictionary for later JSON export (only mssg type 1)
            if decoded_message["msg_type"] == 1:
                if decoded_message["mmsi"] in total_dictionary.keys():
                    total_dictionary[decoded_message["mmsi"]].update({ list(total_dictionary[decoded_message["mmsi"]].keys())[-1] + 1 : { "Repeat" : decoded_message["repeat"], "Status": decoded_message["status"], "Turn": decoded_message["turn"], "Speed": decoded_message["speed"], "Accuracy": decoded_message["accuracy"], "Latitude": decoded_message["lat"], "Longitude": decoded_message["lon"], "Course": decoded_message["course"], "Heading": decoded_message["heading"], "Year" : year, "Month" : month, "Day" : day, "Hour" : hour, "Timestamp": decoded_message["second"], "Maneuver": decoded_message["maneuver"], "Raim": decoded_message["raim"], "Radio": decoded_message["radio"] } } )
                else:
                    total_dictionary[decoded_message["mmsi"]].update({ 0 : { "Repeat" : decoded_message["repeat"], "Status": decoded_message["status"], "Turn": decoded_message["turn"], "Speed": decoded_message["speed"], "Accuracy": decoded_message["accuracy"], "Latitude": decoded_message["lat"], "Longitude": decoded_message["lon"], "Course": decoded_message["course"], "Heading": decoded_message["heading"], "Year" : year, "Month" : month, "Day" : day, "Hour" : hour, "Timestamp": decoded_message["second"], "Maneuver": decoded_message["maneuver"], "Raim": decoded_message["raim"], "Radio": decoded_message["radio"] } } )
        
        except:
            print("Error reading message from file: ", str(file))

        # Count total number of messages
        num_messages += 1
    return num_messages


def save_dictionary_to_json(total_dictionary, oneFile, outputPath):
    """
    """
    if oneFile:
        # Save entire dictionary to one JSON file
        with open("vessel_information.json", 'w') as fp:
            json.dump(total_dictionary, fp, indent=4)
    else:
        # Save JSON file per vessel
        for key, value in total_dictionary.items():
            with open(outputPath + str(key)+".json", 'w') as fp:
                json.dump(value, fp, indent=4)


def save_dictionary_to_csv(total_dictionary, outputPath):
    """
    """
    for mmsi, messageList in total_dictionary.items():
        with open(outputPath + str(mmsi)+".csv", 'a') as fp:
            fp.write("MessageNO" 
                     + "," + "Repeat" 
                     + "," + "Status" 
                     + "," + "Turn"
                     + "," + "Speed"
                     + "," + "Accuracy"
                     + "," + "Latitude"
                     + "," + "Longitude"
                     + "," + "Course"
                     + "," + "Heading"
                     + "," + "Year"
                     + "," + "Month"
                     + "," + "Day"
                     + "," + "Hour"
                     + "," + "Timestamp"
                     + "," + "Maneuver"
                     + "," + "Raim"
                     + "," + "Radio"
                     + "\n")
            for messageCount, message in messageList.items():
                fp.write(str(messageCount) 
                         + "," + str(message["Repeat"]   )
                         + "," + str(message["Status" ]  ) 
                         + "," + str(message["Turn"]     )
                         + "," + str(message["Speed"]    )
                         + "," + str(message["Accuracy"] )
                         + "," + str(message["Latitude"])
                         + "," + str(message["Longitude"] )
                         + "," + str(message["Course"]   )  
                         + "," + str(message["Heading"]  )
                         + "," + str(message["Year"]     )
                         + "," + str(message["Month"]    )
                         + "," + str(message["Day"]      )
                         + "," + str(message["Hour"]     )
                         + "," + str(message["Timestamp"])
                         + "," + str(message["Maneuver"] )
                         + "," + str(message["Raim"]     )
                         + "," + str(message["Radio"]    )
                         + "\n")
                # Debug
                #print(mmsi, "\n", messageCount, "\n", message, "\n\n\n")


def get_log_files_list(path):
    """
        Gets the list of files with the .log extension under a given directory
    """
    logFilesList = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".log"):
                logFilesList.append(os.path.join(root, file))
    return logFilesList