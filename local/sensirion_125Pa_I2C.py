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
ljm.eWriteName(handle, "I2C_SLAVE_ADDRESS", 0x25) # default address is 0x25

ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 1)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 0)

ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", 1, [0x3603])
ljm.eWriteName(handle, "I2C_GO", 1)
time.sleep(0.5)
ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 0)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 9)
ljm.eWriteName(handle, "I2C_GO", 1)

x = 0

if x < 40:
    dataRead = ljm.eReadNameByteArray(handle, "I2C_DATA_RX", 9)
    #print(type(dataRead))
    raw_dp = (dataRead[0] << 8) | dataRead[1]
    # raw_RH = raw_RH & 0x3FFF
    raw_temp = ((dataRead[3] << 8) | dataRead[4])
    # temp = ((dataRead[3] << 8) | dataRead[4]) >> 2
    # scale_factor = temp = ((dataRead[6] << 8) | dataRead[7])

    dp_scale = 240 # Pa^-1
    temp_scale = 2000 # degrees C^-1

    dp = raw_dp / dp_scale
    temp = raw_temp / temp_scale

    print("delta pressure: ", dp)
    print("temperature: ", temp)

    rho = 1.197
    A2 = 3.1415*((0.0508/2)^2)
    v2 = ((2*dp)/(rho*(1-(0.6135^4))))^0.5
    Re = rho*v2*0.0508/0.0000179
    Cd = 1.0054-(6.88*(Re^-0.5))
    Q = Cd*A2*v2 # m3/s
    Q_cfm = Q*2118.88 # CFM
    
    print("Flow (m3/s): ", Q)
    print("Flow (CFM): ", Q_cfm)

    x += 1

ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 1)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 0)

ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", 1, [0x3FF9])
ljm.eWriteName(handle, "I2C_GO", 1)

