from labjack import ljm
import time
import struct
import sys
import math


def time_to_next(sec: float) -> float:
    now = time.time()
    delta = sec - (math.fmod(now, sec))
    return delta

handle = ljm.openS("ANY", "ANY", "10.55.169.207")
info = ljm.getHandleInfo(handle)
print("Opened a LabJack with Device type: %i, Connection type: %i,\n"
      "Serial number: %i, IP address: %s, Port: %i,\nMax bytes per MB: %i" %
      (info[0], info[1], info[2], ljm.numberToIP(info[3]), info[4], info[5]))

deviceType = info[0]

# DC = 25% = 100 * CONFIG_A / 8000
# CONFIG_A = 25 * 8000 / 100 = 2000
CONFIG_A = 10 * 3200 / 100 

ljm.eWriteName(handle, "DIO_EF_CLOCK0_ENABLE", 0)
ljm.eWriteName(handle, "DIO_EF_CLOCK0_DIVISOR", 1)
ljm.eWriteName(handle, "DIO_EF_CLOCK0_ROLL_VALUE", 3200)
ljm.eWriteName(handle, "DIO_EF_CLOCK0_ENABLE", 1)

ljm.eWriteName(handle, "DIO0_EF_ENABLE", 0)
ljm.eWriteName(handle, "DIO0_EF_INDEX", 0)
ljm.eWriteName(handle, "DIO0_EF_CONFIG_A", CONFIG_A)
ljm.eWriteName(handle, "DIO0_EF_ENABLE", 1)

ljm.eWriteName(handle, "DIO1_EF_ENABLE", 0)
ljm.eWriteName(handle, "DIO1_EF_INDEX", 8)
ljm.eWriteName(handle, "DIO1_EF_ENABLE", 1)

while True:
      count1 = ljm.eReadName(handle, "DIO1_EF_READ_A")
      time.sleep(5)
      count2 = ljm.eReadName(handle, "DIO1_EF_READ_A")
      delta_count = count2 - count1
      revs = delta_count/2
      speed = 60*revs/5
      print(speed)