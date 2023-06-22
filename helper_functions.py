import os
import sys
import json
from collections import defaultdict
from pyais import decode
from pyais.stream import FileReaderStream
from tqdm import tqdm


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
                        
            # Write messages to dictionary for later JSON export (only mssg type 1)
            if decoded_message["msg_type"] == 1:
                if decoded_message["mmsi"] in total_dictionary.keys():
                    total_dictionary[decoded_message["mmsi"]].update({ list(total_dictionary[decoded_message["mmsi"]].keys())[-1] + 1 : { "Repeat" : decoded_message["repeat"], "Status": decoded_message["status"], "Turn": decoded_message["turn"], "Speed": decoded_message["speed"], "Accuracy": decoded_message["accuracy"], "Longitude": decoded_message["lon"], "Latitude": decoded_message["lat"], "Course": decoded_message["course"], "Heading": decoded_message["heading"], "Timestamp": decoded_message["second"], "Maneuver": decoded_message["maneuver"], "Raim": decoded_message["raim"], "Radio": decoded_message["radio"] } } )
                else:
                    total_dictionary[decoded_message["mmsi"]].update({ 0 : { "Repeat" : decoded_message["repeat"], "Status": decoded_message["status"], "Turn": decoded_message["turn"], "Speed": decoded_message["speed"], "Accuracy": decoded_message["accuracy"], "Longitude": decoded_message["lon"], "Latitude": decoded_message["lat"], "Course": decoded_message["course"], "Heading": decoded_message["heading"], "Timestamp": decoded_message["second"], "Maneuver": decoded_message["maneuver"], "Raim": decoded_message["raim"], "Radio": decoded_message["radio"] } } )
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


def save_dictionary_to_csv(total_dictionary):
    """
    """
    
    pass


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