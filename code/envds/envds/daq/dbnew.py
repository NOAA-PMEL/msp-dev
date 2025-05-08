import os
import asyncio
from redis_om import get_redis_connection
from aredis_om import (
    EmbeddedJsonModel,
    JsonModel,
    Field,
    Migrator,
    # get_redis_connection,
    NotFoundError,
)
from pydantic import ValidationError
from envds.util.util import get_checksum

from envds.util.util import get_datetime_string

# from envds.daq.sensor import Sensor

from envds.db.db_redis import init_model_type, save_model, delete_model, expire_model
from datetime import datetime

# TODO: how to abstract to allow redis, mongo, etc as db?

# def get_connection():
#     return get_redis_connection(url="redis://redis.default")


class DeviceTypeRegistration(JsonModel):
    make: str = Field(index=True)
    model: str = Field(index=True)
    version: str = Field(index=True)
    checksum: str
    creation_date = str
    metadata: dict = {}

    # class Meta:
    #     database = get_connection()


class DeviceRegistration(JsonModel):
    make: str = Field(index=True)
    model: str = Field(index=True)
    serial_number: str = Field(index=True)
    source_id: str
    creation_date = datetime

class DeviceDataRecord(JsonModel):
    make: str = Field(index=True)
    model: str = Field(index=True)
    serial_number: str = Field(index=True)
    timestamp: datetime = Field(index=True)
    source_id: str = Field(index=True)
    data: dict

    # class Meta:
    #     database = get_connection()


async def init_db_models():
    # print(f"init_sensor_type_registration: {SensorTypeRegistration.Meta.database}")
    # await init_model_type(SensorTypeRegistration)
    # print("init_sensor_type_registration: done")
    # SensorTypeRegistration.Meta.database = await get_redis_connection(
    #     url="redis://redis.default"
    # )
    await Migrator().run()


async def init_device_registration():
    await init_model_type(DeviceRegistration)


# async def register_sensor_type_as_sensor(sensor: Sensor, version: str = "1.0.0"):
#     await register_sensor_type(
#         make=sensor.get_make(),
#         model=sensor.get_model(),
#         version=version,
#         metadata=sensor.get_sensor_type_metadata()
#     )


async def register_device_type(
    make: str, model: str, version: str = "1.0.0", creation_date: str = None, metadata: dict = {}
):
    print(f'redis url: {os.getenv("REDIS_OM_URL")}')
    try:
        print("here:1")
        if creation_date is None:
            # creation_date = datetime.utcnow()
            creation_date = get_datetime_string()
        reg = DeviceTypeRegistration(
            make=make,
            model=model,
            version=version,
            checksum=get_checksum(metadata),
            creation_date=creation_date,
            metadata=metadata,
        )
        print("here:2")
        current = await get_device_type_registration(
            make=make, model=model, version=version
        )
        print(f"make: {make}, model: {model}, version: {version}")
        print("here:3")
        # current = await SensorTypeRegistration.find(
        #     SensorTypeRegistration.make == make
        #     and SensorTypeRegistration.model == model
        #     and SensorTypeRegistration.version == version
        # ).first()
        print(f"current: {current}")
        if current:
            if current.checksum == reg.checksum:
                print(f"current exists: {current}, {current.creation_date}, {reg.creation_date}")
                print(f"compare: {current.creation_date > reg.creation_date}")
                return
            else:
                try:
                    if current.creation_date > reg.creation_date:
                        print("current registration is newer")
                        return
                except TypeError as e:
                    print(f"registration typeerror: {e}")
                    pass
                await DeviceTypeRegistration.delete(current.pk)

        print(f"reg({reg.pk}): {reg}")
        await reg.save()
        # await save_model(reg)
    except (ValidationError, Exception) as e:
        print(f"Could not register device: \n{e}")


# TODO: abstract the find method?
async def get_device_type_registration(
    make: str, model: str, version: str = "1.0.0"
) -> DeviceTypeRegistration:
    try:
        reg = await DeviceTypeRegistration.find(
            DeviceTypeRegistration.make == make,
            DeviceTypeRegistration.model == model,
            DeviceTypeRegistration.version == version
        ).first()
        print(f"{make}-{model}-{version}: {reg}")
        return reg
    except NotFoundError as e:
        print(f"get_device_type error: {e}")
        return None


async def get_device_type_metadata(make: str, model: str, version: str = "1.0.0") -> dict:
    reg = await get_device_type_registration(make=make, model=model, version=version)
    if reg:
        return reg.metadata
    return {}


async def register_device(
    make: str, model: str, serial_number: str, source_id: str, expire: int = 300
):
    try:

        reg = DeviceRegistration(
            make=make,
            model=model,
            serial_number=serial_number,
            creation_date=datetime.now(datetime.timezone.utc),
            source_id=source_id,
        )

        current = await get_device_registration(
            make=make,
            model=model,
            serial_number=serial_number,
        )
        # current = await SensorRegistration.find(
        #     SensorRegistration.make == make
        #     and SensorRegistration.model == model
        #     and SensorRegistration.serial_number == serial_number
        # ).first()
        print(f"current: {current}")
        if current:
            await current.expire(expire)
            return

        print(f"reg({reg.pk}): {reg}")
        await reg.save()
        # await save_model(reg)
        await reg.expire(expire)
        # await expire_model(reg, expire=expire)
    except (ValidationError, Exception) as e:
        print(f"Could not register device: \n{e}")


async def get_device_registration(
    make: str, model: str, serial_number: str
) -> DeviceRegistration:
    try:
        reg = await DeviceRegistration.find(
            DeviceRegistration.make == make,
            DeviceRegistration.model == model,
            DeviceRegistration.serial_number == serial_number
        ).first()
        return reg

    except NotFoundError as e:
        print(f"get_device error: {e}")
        return None

async def get_device_type_registration_by_pk(pk) -> DeviceTypeRegistration:
    try:
        reg = DeviceTypeRegistration.get(pk)
        return reg
    except NotFoundError:
        return None

async def get_device_registration_by_pk(pk) -> DeviceRegistration:
    try:
        reg = await DeviceRegistration.get(pk)
        return reg
    except NotFoundError:
        return None

async def get_all_device_registration() -> list[DeviceRegistration]:
    try:
        regs = await DeviceRegistration.find().all()
        return regs

    except NotFoundError as e:
        print(f"get_device error: {e}")
        return None

async def get_all_device_type_registration() -> list[DeviceTypeRegistration]:
    try:
        regs = await DeviceTypeRegistration.find().all()
        return regs

    except NotFoundError as e:
        print(f"get_device_type error: {e}")
        return None

async def save_device_data(
    make: str, model: str, serial_number: str, source_id: str, timestamp: str, data: dict, expire: int = 300
):

    try:
        record = DeviceDataRecord(
            make=make,
            model=model,
            serial_number=serial_number,
            timestamp=timestamp,
            source_id=source_id,
            data=data
        )
        print(f"record: {record}")
        await record.save()
        await record.expire(expire)
    except Exception as e:
        print(f"save_device_data error: {e}")

async def get_device_data(
    make: str, model: str, serial_number: str, start_time: str = None, stop_time: str = None
) -> list[DeviceDataRecord]:
    try:
        data = await DeviceDataRecord.find(
            DeviceDataRecord.make == make,
            DeviceDataRecord.model == model,
            DeviceDataRecord.serial_number == serial_number
        ).sort_by("timestamp").all()
        return data

    except NotFoundError as e:
        print(f"get_device_data error: {e}")
        return None
