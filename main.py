import os
import sys
import json
from collections import defaultdict
from pyais import decode
from pyais.stream import FileReaderStream
from tqdm import tqdm

def export_messages_with_multi_part(inputPath, outputPath, logFilesList):
    """
        This function takes an input path, which is the log file with the encoded AIS messages, 
        and an output path, where the decoded messages will be written as a JSON file.
        It takes the messages from the said log file, and decodes them, which will then export them to a JSON file.

        inputPath: str: Encoded AIS messages path (Usually .txt or .log)
        outputPath: str: JSON Export (Will be converted to JSON format if not provided in that format)
    """

    # Variables initialization
    num_messages = 0
    total_dictionary = defaultdict(dict)
    msg_type_count = [0]*27


    # For loop to go through file and decode each message. Adjust total accordingly
    for file in tqdm(logFilesList, total = 72):
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

    # Debug
    print("Total number of messages: ", num_messages)
    #for i in range(0, 27):
    #    print("Total number of type ", str(i+1), " messages: ", msg_type_count[i])
    
    # Save entire dictionary to one JSON file
    #with open(outputPath, 'w') as fp:
    #    json.dump(total_dictionary, fp, indent=4)

    # Save JSON file per vessel
    for key, value in total_dictionary.items():
        with open("VesselJSONs\\"+str(key)+".json", 'w') as fp:
            json.dump(value, fp, indent=4)


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
    

# MAIN
if __name__ == "__main__":
    logFilesList = get_log_files_list(sys.argv[1])
    export_messages_with_multi_part(sys.argv[1], sys.argv[2], logFilesList)