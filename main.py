from helper_functions import *

def export_messages_with_multi_part(outputPath, logFilesList):
    """
        This function takes an input path, which is the log file with the encoded AIS messages, 
        and an output path, where the decoded messages will be written as a JSON file.
        It takes the messages from the said log file, and decodes them, which will then export them to a JSON file.

        outputPath: str: JSON Export (Will be converted to JSON format if not provided in that format)
    """

    # Variables initialization
    num_messages = 0
    total_dictionary = defaultdict(dict)
    msg_type_count = [0]*27
    fileCount = len(logFilesList)

    # For loop to go through file and decode each message. Adjust total accordingly
    for file in tqdm(logFilesList, total = fileCount):
        num_messages = decode_given_file(file, total_dictionary, num_messages)
        #break # TODO Remove to decode all files

    # Debug
    print("Total number of messages: ", num_messages)
    #for i in range(0, 27):
    #    print("Total number of type ", str(i+1), " messages: ", msg_type_count[i])
    
    #save_dictionary_to_json(total_dictionary, False, outputPath)
    save_dictionary_to_csv(total_dictionary, outputPath)


# MAIN
if __name__ == "__main__":
    logFilesList = get_log_files_list(sys.argv[1])
    export_messages_with_multi_part(sys.argv[2], logFilesList)