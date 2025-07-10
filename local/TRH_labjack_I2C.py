from labjack import ljm
import time
import struct

handle = ljm.openS("ANY", "ANY", "10.55.169.207")
info = ljm.getHandleInfo(handle)
print("Opened a LabJack with Device type: %i, Connection type: %i,\n"
      "Serial number: %i, IP address: %s, Port: %i,\nMax bytes per MB: %i" %
      (info[0], info[1], info[2], ljm.numberToIP(info[3]), info[4], info[5]))

deviceType = info[0]


ljm.eWriteName(handle, "I2C_SDA_DIONUM", 2)  # CS is FIO2
ljm.eWriteName(handle, "I2C_SCL_DIONUM", 3)  # CLK is FIO3
ljm.eWriteName(handle, "I2C_SPEED_THROTTLE", 65516) # CLK frequency approx 100 kHz
ljm.eWriteName(handle, "I2C_OPTIONS", 0)
ljm.eWriteName(handle, "I2C_SLAVE_ADDRESS", 0x28) # default address is 0x28 (40 decimal)

ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 1)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 0)

ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", 1, [0x00])
#ljm.eWriteNameArray(handle, "I2C_DATA_TX", 1, [0x00])
#ljm.eWriteName(handle, "I2C_DATA_TX", 0x00)
ljm.eWriteName(handle, "I2C_GO", 1)
time.sleep(0.5)
#dataRead = ljm.eReadNameByteArray(handle, "I2C_DATA_RX", 4)
ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 0)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 4)
ljm.eWriteName(handle, "I2C_GO", 1)

dataRead = ljm.eReadNameByteArray(handle, "I2C_DATA_RX", 4)
#print(ljm.eReadName(handle, "I2C_ACKS"))
#print(type(dataRead))
raw_RH = (dataRead[0] << 8) | dataRead[1]
raw_RH = raw_RH & 0x3FFF
#print(dataRead[0], dataRead[0] << 8)
raw_temp = ((dataRead[2] << 8) | dataRead[3]) >> 2

#temp = temp >> 2
# print(bin(96))
# print(bin(96<<8))
# print(bin(72))
# print(bin(96<<8 | 72))
# hum = dataRead[0]| dataRead[1]
# print(bin(hum))
# hum = hum & 0x3FFF
# temp = dataRead[2] | dataRead[3]
# temp = temp >> 2
RH = (raw_RH/16383)*100
temp = (raw_temp*165/16383)-40
print('Relative Humidity (%):', round(RH,2))
print('Temperature (C):', round(temp,2))
# humidity_raw = struct.unpack('>H', bytes(dataRead[0:2]))[0]
# humidity = (humidity_raw/16383)*100
# print(humidity)

# temp_raw = struct.unpack('>H', bytes(dataRead[2:4]))[0]
# temp2 = ((temp_raw /16383)*165) - 40
# print(temp2)


#ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 4)
# ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", 1, [0xA0])
# ljm.eWriteName(handle, "I2C_GO", 1)


# dataRead = ljm.eReadNameByteArray(handle, "I2C_DATA_RX", 4)
# print(dataRead)
# print(ljm.eReadName(handle, "I2C_ACKS"))
