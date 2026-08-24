from labjack import ljm
import time
import struct

handle = ljm.openS("ANY", "ETHERNET", "ANY")
info = ljm.getHandleInfo(handle)
print("Opened a LabJack with Device type: %i, Connection type: %i,\n"
      "Serial number: %i, IP address: %s, Port: %i,\nMax bytes per MB: %i" %
      (info[0], info[1], info[2], ljm.numberToIP(info[3]), info[4], info[5]))

deviceType = info[0]


ljm.eWriteName(handle, "I2C_SDA_DIONUM", 4)  # CS is FIO2
ljm.eWriteName(handle, "I2C_SCL_DIONUM", 5)  # CLK is FIO3
ljm.eWriteName(handle, "I2C_SPEED_THROTTLE", 0) # CLK frequency approx 100 kHz
ljm.eWriteName(handle, "I2C_OPTIONS", 0)
ljm.eWriteName(handle, "I2C_SLAVE_ADDRESS", 0x25) # default address is 0x25

ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 2)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 0)

ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", 2, [0x36, 0x08])
ljm.eWriteName(handle, "I2C_GO", 1)
time.sleep(0.5)
ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 0)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 9)
ljm.eWriteName(handle, "I2C_GO", 1)

x = 0

while x < 40:
    ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 0)
    ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 9)
    ljm.eWriteName(handle, "I2C_GO", 1)

    dataRead = ljm.eReadNameByteArray(handle, "I2C_DATA_RX", 9)
    print((dataRead))
    raw_dp = (dataRead[0] << 8) | dataRead[1]
    if raw_dp & 0x8000:
        raw_dp -= 65536
    if raw_dp < 1:
       raw_dp = raw_dp/-1
    # raw_RH = raw_RH & 0x3FFF
    raw_temp = ((dataRead[3] << 8) | dataRead[4])
    if raw_temp & 0x8000:
        raw_temp -= 65536
   # temp = ((dataRead[3] << 8) | dataRead[4]) >> 2
    # scale_factor = temp = ((dataRead[6] << 8) | dataRead[7])

    dp_scale = 240 # Pa^-1
    temp_scale = 200 # degrees C^-1

    dp = raw_dp / dp_scale
    temp = raw_temp / temp_scale

    print("delta pressure: ", dp)
    print("temperature: ", temp)

    rho = 1.297
    A2 = 3.1415*((0.0508/2.0)**2.0)
    v2 = ((2.0*dp)/(rho*(1.0-(0.6135**4.0))))**0.5
    Q_test = v2*(3.1415*(0.0508/2.0)**2.0)
    print(Q_test*2118.88)
    Re = rho*v2*0.0508/0.0000179
    Cd = 1.0054-(6.88*(Re**-0.5))
    Q = Cd*A2*v2 # m3/s
    Q_cfm = Q*2118.88 # CFM
    
    print("Flow (m3/s): ", Q)
    print("")
    print("Flow (CFM): ", Q_cfm)

    x += 1
    time.sleep(0.5)

ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 1)
ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 0)

ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", 1, [0x3FF9])
ljm.eWriteName(handle, "I2C_GO", 1)

