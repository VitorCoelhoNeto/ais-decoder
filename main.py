from pyais import decode
from pyais.stream import FileReaderStream
import os

TEST_PATH = "D:\\TMDEI\\201501\\ais_mssis_2015-01-01_00h.log\\ais_mssis_2015-01-01_00h.log"

if __name__ == "__main__":


    for msg in FileReaderStream("D:\\TMDEI\\201501\\ais_mssis_2015-01-01_00h.log\\ais_mssis_2015-01-01_00h.log"):
        decoded_message = msg.decode()

        #print(decoded_message.msg_type)
        try:
            print(decoded_message.speed)
        except:
            print("No attribute type speed")











###### OLD TESTS ######

#decoded = decode(b"!AIVDM,1,1,,B,15NG6V0P01G?cFhE`R2IU?wn28R>,0*05")
#decoded = decode(b"!AIVDM,1,1,,B,39NS>0Q00305ld`3NjbK@kmkP000,0*40,1420070398")
#decoded = decode(b"!AIVDM,2,1,1,A,53cmTc01vbILT<tl000thB1LTpD000000000001I000000A@0042DQEDh000,0*3B,1420070398")
#print(decoded)

#fileList = os.walk(TEST_PATH)
#
#for file in fileList:
#    print(file)

# Actual file test
#file = open(TEST_PATH, "r", encoding="utf-8")
#for line in file:
#    print(line)
#    print(type(line))
#    decoded = decode(line)
#    print(decoded)