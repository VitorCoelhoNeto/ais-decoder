import os
import sys
import json
from collections import defaultdict
from pyais import decode
from pyais.stream import FileReaderStream
from tqdm import tqdm

def export_messages_with_multi_part(inputPath, outputPath):
    """
        This function takes an input path, which is the log file with the encoded AIS messages, 
        and an output path, where the decoded messages will be written as a JSON file.
        It takes the messages from the said log file, and decodes them, which will then export them to a JSON file.

        inputPath: str: Encoded AIS messages path (Usually .txt or .log)
        outputPath: str: JSON Export (Will be converted to JSON format if not provided in that format)
    """
    if not outputPath.endswith(".json") :
        outputPath = outputPath + ".json"
    num_messages = 0
    total_dictionary = defaultdict(dict)

    for msg in tqdm(FileReaderStream(inputPath), total=447869):
        decoded_message = msg.decode().asdict()
        #print("\n", decoded_message, "\n")

        if decoded_message["msg_type"] == 3:
            if decoded_message["mmsi"] in total_dictionary.keys():
                total_dictionary[decoded_message["mmsi"]].update({ list(total_dictionary[decoded_message["mmsi"]].keys())[-1] + 1 : decoded_message["speed"]})
            else:
                total_dictionary[decoded_message["mmsi"]].update({ 0 : decoded_message["speed"]})

        num_messages += 1

    print("Total number of messages: ", num_messages)

    with open(outputPath, 'w') as fp:
        json.dump(total_dictionary, fp, indent=4)
    

# MAIN
if __name__ == "__main__":
    export_messages_with_multi_part(sys.argv[1], sys.argv[2])

















###################
#     ARCHIVE     #
###################

TEST_PATH = "D:\\TMDEI\\201501\\ais_mssis_2015-01-01_00h.log\\ais_mssis_2015-01-01_00h.log"

def decode_test_path():

    with open(TEST_PATH, 'r') as fp:
        for msg in tqdm(FileReaderStream(TEST_PATH), total=len(fp.readlines())):
            decoded_message = msg.decode()

            print(decoded_message.msg_type)
            try:
                print(decoded_message.speed)
            except:
                print("No attribute type speed")


def testing():
    decoded = decode(b"!AIVDM,1,1,,B,15NG6V0P01G?cFhE`R2IU?wn28R>,0*05")
    decoded = decode(b"!AIVDM,1,1,,B,39NS>0Q00305ld`3NjbK@kmkP000,0*40,1420070398")
    decoded = decode(b"!AIVDM,2,1,1,A,53cmTc01vbILT<tl000thB1LTpD000000000001I000000A@0042DQEDh000,0*3B,1420070398")
    print(decoded)

    fileList = os.walk(TEST_PATH)
    for file in fileList:
        print(file)

    # Actual file test
    file = open(TEST_PATH, "r", encoding="utf-8")
    for line in file:
        print(line)
        print(type(line))
        decoded = decode(line)
        print(decoded)


def testing_message_types(message_id):
    message_dictionary = {
        "MESSAGE_1" : "!AIVDM,1,1,,B,39NS>0Q00305ld`3NjbK@kmkP000,0*40,1420070398",
        "MESSAGE_2" : "!AIVDM,2,1,1,A,53cmTc01vbILT<tl000thB1LTpD000000000001I000000A@0042DQEDh000,0*3B,1420070398",
        "MESSAGE_3" : "!AIVDM,2,2,1,A,00000000000,2*25,1420070398"
    }

    as_dict = decode(message_dictionary[message_id]).asdict()
    print("\n", as_dict, "\n")
    print(as_dict["mmsi"])
    #print("\n", decode(MESSAGE_1), "\n")


def testing_ais_tracker():
    import pathlib
    from pyais import AISTracker

    filename = "D:\\TMDEI\\201501\\ais_mssis_2015-01-01_00h.log\\ais_mssis_2015-01-01_00h.log"#pathlib.Path(__file__).parent.joinpath('sample.ais')

    with AISTracker() as tracker:
        for msg in tqdm(FileReaderStream(str(filename)), total=487000):
            tracker.update(msg)
            latest_tracks = tracker.n_latest_tracks(10)

    # Get the latest 10 tracks
    print('latest 10 tracks', ','.join(str(t.mmsi) for t in latest_tracks))

    # Get a specific track
    print(tracker.get_track(247293100)) # Is this the latest update on this specific MMSI? (TODO)