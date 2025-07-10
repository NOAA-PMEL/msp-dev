from labjack import ljm
from random import randrange
import time
import struct

handle = ljm.openS("ANY", "ANY", "10.55.169.207")
info = ljm.getHandleInfo(handle)
print("Opened a LabJack with Device type: %i, Connection type: %i,\n"
      "Serial number: %i, IP address: %s, Port: %i,\nMax bytes per MB: %i" %
      (info[0], info[1], info[2], ljm.numberToIP(info[3]), info[4], info[5]))

deviceType = info[0]

# Setting CS, CLK, MISO, and MOSI lines for the T7 and T8.
ljm.eWriteName(handle, "SPI_CS_DIONUM", 0)  # CS is FIO0
ljm.eWriteName(handle, "SPI_CLK_DIONUM", 1)  # CLK is FIO1
ljm.eWriteName(handle, "SPI_MISO_DIONUM", 2)  # MISO is FIO2
ljm.eWriteName(handle, "SPI_MOSI_DIONUM", 3)  # MOSI is FIO3

# Selecting Mode CPHA=1 (bit 0), CPOL=1 (bit 1)
ljm.eWriteName(handle, "SPI_MODE", 1)

# Speed Throttle:
# Valid speed throttle values are 1 to 65536 where 0 = 65536.
# Configuring Max. Speed (~800 kHz) = 0
ljm.eWriteName(handle, "SPI_SPEED_THROTTLE", 65530)

# SPI_OPTIONS:
# bit 0:
#     0 = Active low clock select enabled
#     1 = Active low clock select disabled.
# bit 1:
#     0 = DIO directions are automatically changed
#     1 = DIO directions are not automatically changed.
# bits 2-3: Reserved
# bits 4-7: Number of bits in the last byte. 0 = 8.
# bits 8-15: Reserved

# Enabling active low clock select pin
ljm.eWriteName(handle, "SPI_OPTIONS", 0)

# Read back and display the SPI settings
def settings():
    aNames = ["SPI_CS_DIONUM", "SPI_CLK_DIONUM", "SPI_MISO_DIONUM",
            "SPI_MOSI_DIONUM", "SPI_MODE", "SPI_SPEED_THROTTLE",
            "SPI_OPTIONS"]
    aValues = [0]*len(aNames)
    numFrames = len(aNames)
    aValues = ljm.eReadNames(handle, numFrames, aNames)

    print("\nSPI Configuration:")
    for i in range(numFrames):
        print("  %s = %0.0f" % (aNames[i],  aValues[i]))


# def send_cmd(cmd_byte, com_byte_array):
#     dataRead = [None]
#     i = 0
#     serial_list = []

#     while dataRead[0] != 243:
#         numBytes = 1
#         ljm.eWriteName(handle, "SPI_NUM_BYTES", numBytes)
#         ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [cmd_byte])
#         ljm.eWriteName(handle, "SPI_GO", 1)
#         dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
#         print(dataRead)
#         time.sleep(0.03)
    
#     while i < 60:
#         i += 1
#         numBytes = len(com_byte_array)
#         ljm.eWriteName(handle, "SPI_NUM_BYTES", numBytes)
#         ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", numBytes, com_byte_array)
#         ljm.eWriteName(handle, "SPI_GO", 1)
#         dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", numBytes)
#         print(dataRead)
#         serial_list.append(chr(dataRead[0]))
#         time.sleep(0.03)
#     print(serial_list)


# send_cmd(0x10,[0x10])

def send_command(cmd, interval=10e-6):
    numBytes = 1
    ljm.eWriteName(handle, "SPI_NUM_BYTES", numBytes)
    ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [cmd])
    ljm.eWriteName(handle, "SPI_GO", 1)
    dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
    #print(dataRead)
    time.sleep(interval)
    return dataRead[0]


def send_command_and_wait(cmd):
    attempts = 0
    dataRead = 49

    while (dataRead != 243):

        if dataRead != 49:
            time.sleep(5)

        if attempts > 20:
            time.sleep(5)
        
        dataRead = send_command(cmd, interval = 0.02)
        attempts = attempts + 1


def read_bytes(cmd, sz):
    buf = []
    send_command_and_wait(cmd)
    for i in range(sz):
        buf += [send_command(cmd)]
    result = bytearray(buf)
    return result


def write_bytes(cmd, buf):
    send_command_and_wait(cmd)
    for c in buf:
        send_command(c)

# Turn fan on
write_bytes(0x03,[0x03])
time.sleep(1)

# Turn laser on
write_bytes(0x03, [0x07])  
time.sleep(1)

time.sleep(3)

def get_serial_str():
    dataRead = read_bytes(0x10, 60)
    print(dataRead.decode('ascii'))

#get_serial_str()

def get_PM_data():
    dataRead = read_bytes(0x32,14)
    PM_arrays = [dataRead[i:i+4] for i in range(0, len(dataRead), 4)]
    #print(dataRead)
    for array in PM_arrays[0:3]:
        print(array)
        PM_val = struct.unpack("f", array)
        print(PM_val)

get_PM_data()

def get_hist_data():
    dataRead = read_bytes(0x30,86)
    bin_counts = [dataRead[i:i+24] for i in range(0, len(dataread), 1)]
    bin_MToF = [dataRead[i+24:i+]]
    PM_arrays = [dataRead[i:i+4] for i in range(0, len(dataRead), 4)]
    #print(dataRead)
    for array in PM_arrays[0:3]:
        print(array)
        PM_val = struct.unpack("f", array)
        print(PM_val)


# def get_PM_data():
#     dataRead = [None]
#     i = 0
#     PM_data_list = []
#     while i < 16:
#         i += 1
#         ljm.eWriteName(handle, "SPI_NUM_BYTES", 1)
#         ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [0x32])
#         ljm.eWriteName(handle, "SPI_GO", 1)
#         dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
#         print(dataRead)
#         time.sleep(0.01)   

    # while 2 <= i <= 4:
    #     i += 1
    #     ljm.eWriteName(handle, "SPI_NUM_BYTES", 4)
    #     ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 4, [0x32,0x32,0x32,0x32])
    #     ljm.eWriteName(handle, "SPI_GO", 1)
    #     dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 4)
    #     print(ljm.byteArrayToFLOAT32(dataRead))
    #     print(dataRead)
    #     print(int.from_bytes(dataRead))
    #     time.sleep(0.01)
    #print(PM_data_list[2:])


#get_PM_data()

# ljm.eWriteName(handle, "SPI_NUM_BYTES", 1)
# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [0x32])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
# print(dataRead)
# time.sleep(0.01)

# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [0x32])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
# print(dataRead)
# time.sleep(0.01)

# ljm.eWriteName(handle, "SPI_NUM_BYTES", 4)
# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 4, [0x32,0x32,0x32,0x32])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 4)
# print(dataRead)
# time.sleep(0.01)

# ljm.eWriteName(handle, "SPI_NUM_BYTES", 4)
# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 4, [0x32,0x32,0x32,0x32])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 4)
# print(dataRead)
# time.sleep(0.01)

# ljm.eWriteName(handle, "SPI_NUM_BYTES", 4)
# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 4, [0x32,0x32,0x32,0x32])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 4)
# print(dataRead)
# time.sleep(0.01)

#send_cmd(0x10,[0x10])

# Turn fan on
#get_response(0x03, 0x02)

# Turn laser on
#send_cmd(0x03, 0x07)


# numBytes = 1
# ljm.eWriteName(handle, "SPI_NUM_BYTES", numBytes)

# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [0x03])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
# print(f"command result: {dataRead}")
# time.sleep(0.03)

# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [0x03])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
# print(f"fan result: {dataRead}")
# time.sleep(0.00005)

# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 1, [0x02])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 1)
# print(f"fan result: {dataRead}")
# time.sleep(4)

# ljm.eWriteNameByteArray(handle, "SPI_DATA_TX", 2, [0x03,0x02])
# ljm.eWriteName(handle, "SPI_GO", 1)
# dataRead = ljm.eReadNameByteArray(handle, "SPI_DATA_RX", 2)
# print(f"fan result: {dataRead}")
# time.sleep(3)

# Close handle
ljm.close(handle)