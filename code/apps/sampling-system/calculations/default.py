import math
import logging

logger = logging.getLogger(__name__)

async def calculate_true_wind_speed(self, relative_wind_speed=None, relative_wind_direction=None, platform_speed=None, platform_heading=None):
    """
    Calculates true wind speed based on apparent (relative) wind and platform vectors.
    """
    # 1. Fail gracefully if pipeline dependencies haven't generated a value yet
    if any(v is None for v in [relative_wind_speed, relative_wind_direction, platform_speed, platform_heading]):
        return None
        
    try:
        # Note: If your platform_speed (SOG) is in km/h, convert it to m/s here:
        # platform_speed_ms = platform_speed / 3.6
        platform_speed_ms = platform_speed

        # 2. Convert apparent wind angle to radians 
        # (Assuming relative_wind_direction is 0 when wind is directly on the bow)
        rel_wd_rad = math.radians(relative_wind_direction)
        
        # 3. Calculate True Wind Speed using the Law of Cosines
        # TWS^2 = AWS^2 + SOG^2 - 2 * AWS * SOG * cos(Apparent Wind Angle)
        tws_squared = (relative_wind_speed ** 2) + (platform_speed_ms ** 2) - (2 * relative_wind_speed * platform_speed_ms * math.cos(rel_wd_rad))
        
        # 4. Use max(0, ...) to protect against negative zero floating point math errors
        true_wind_speed = math.sqrt(max(0, tws_squared))
        
        # Return either a scalar or a dict matching the variable name
        return {"true_wind_speed": round(true_wind_speed, 2)}
        
    except Exception as e:
        self.logger.error("Error calculating true wind speed", extra={"reason": e})
        return None