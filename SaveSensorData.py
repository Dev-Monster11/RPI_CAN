import psycopg2.extras
from psycopg2 import InterfaceError
import requests
import threading
from requests import ConnectionError
import math
import time
import sys
import os
import re
import obd
from datetime import datetime
from multiprocessing import Queue

# from venv import BBSensorModules
from BBSensorModules import LidarModule, GPSModule, Constants, IMU, CANModule
from BBEvents import EventsWatcher

tick_count = 0
deviceJustStarted = 1
canSendUploadRequest = True
accelerometerSensor = None


######################################
speed_queue = Queue()
rpm_queue = Queue()
throttle_position_queue = Queue()
engine_load_queue = Queue()
distance_with_mil_on_queue = Queue()
run_time_since_engine_start_queue = Queue()
fuel_tank_level_input_queue = Queue()
accelerator_pedal_position_D_queue = Queue()
accelerator_pedal_position_E_queue = Queue()
accelerator_pedal_position_F_queue = Queue()
fuel_type_queue = Queue()
engine_fuel_rate_queue = Queue()
driver_demand_engine_queue = Queue()
actual_demand_engine_queue = Queue()
fuel_system_control_queue = Queue()
#######################################



# /////////////// 22.04.2021
last_upload_retry = datetime.now()
# /////////////// 22.04.2021

def parse_calibration_file():
    try:
        f = open("BBSensorModules/sensoroffset.txtISF", "r")

        if f.mode == 'r':
            contents = f.read()
            result = re.search('accel_offset_x=(.*)end', contents)
            if result.group():
                 Constants.ACCEL_OFFSET_X = int(result.group(1))
            result = re.search('accel_offset_y=(.*)end', contents)
            if result.group():
                Constants.ACCEL_OFFSET_Y = int(result.group(1))
            result = re.search('accel_offset_z=(.*)end', contents)
            if result.group():
                Constants.ACCEL_OFFSET_Z = int(result.group(1))

            result = re.search('gyro_offset_x=(.*)end', contents)
            if result.group():
                Constants.GYRO_OFFSET_X = int(result.group(1))
            result = re.search('gyro_offset_y=(.*)end', contents)
            if result.group():
                Constants.GYRO_OFFSET_Y = int(result.group(1))
            result = re.search('gyro_offset_z=(.*)end', contents)
            if result.group():
                Constants.GYRO_OFFSET_Z = int(result.group(1))
            
            print(datetime.now() + 'Sensor offest: ')
            print(Constants.ACCEL_OFFSET_X, Constants.ACCEL_OFFSET_Y, Constants.ACCEL_OFFSET_Z, Constants.GYRO_OFFSET_X, Constants.GYRO_OFFSET_Y, Constants.GYRO_OFFSET_Z)
        f.close()
    except Exception as e:
        print(datetime.now(), "Can not open offset file: " + str(e))
    # except AttributeError:
    #     print "Offset data doesn't exist. Try to tun calibrate() function from Accelerometer and Gyroscop Modules."

def get_can_unread_database_values(database_connection):
        destination_overview_query = """
                      SELECT
                         id,
                         timestamp,
                         speed,
                         rpm,
                         throttle_position,
                         engine_load,
                         distance_with_mil_on, 
                         run_time_since_engine_start, 
                         fuel_tank_level_input, 
                         accelerator_pedal_position_D,
                         accelerator_pedal_position_E, 
                         accelerator_pedal_position_F,
                         fuel_type,
                         driver_demand_engine, 
                         actual_demand_engine, 
                         fuel_system_control, 
                         engine_fuel_rate,
                         session_uuid
                      FROM
                        can
                      WHERE
                        data_sent is FALSE
                       LIMIT %s;
                  """
        cursor = database_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(destination_overview_query, [Constants.DATABASE_ROWS_LIMIT])
        try:
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception, e:
            cursor.close()
            print "CAN ERROR 3: error fetching data from database: ", str(e)
            return []

def set_can_database_values_as_read(database_connection, rows_list):
        if not rows_list:
            print 'CAN ERROR 4: can update list is empty'
            return
        destination_overview_query = """
                            UPDATE
                             can
                            SET 
                             data_sent = TRUE
                            WHERE
                             id IN ({0})
                        """.format(','.join([str(x) for x in rows_list]))
        cursor = database_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(destination_overview_query)
        cursor.close()


def uploadData(database_connection):
    global deviceJustStarted
    global canSendUploadRequest
    # /////////////// 22.04.2021
    global last_upload_retry

    if canSendUploadRequest or (datetime.now() - last_upload_retry).total_seconds() > 30:
        canSendUploadRequest = False
        last_upload_retry = datetime.now()
    else:
        print(datetime.now(), "Waiting for previous request to finish")
        return
    # /////////////// 22.04.2021

    if accelerometerSensor:
        # get acceleration entries
        accel_rows = accelerometerSensor.get_accell_unread_database_values(database_connection)
    else:
        accel_rows = []

    if gpsSensor:
        # get gps entries
        gps_rows = gpsSensor.get_gps_unread_database_values(database_connection)
    else:
        gps_rows = []

    if lidarSensor:
        # get lidar entries
        lidar_rows = lidarSensor.get_lidar_unread_database_values(database_connection)
    else:
        lidar_rows = []

    if canSensor:
        # get can entries
        can_rows = get_can_unread_database_values(database_connection)
    else:
        can_rows = []


    if len(accel_rows):
        min_data_rows_available = min(len(lidar_rows), len(gps_rows), len(can_rows), len(accel_rows))
    else:
        min_data_rows_available = min(len(lidar_rows), len(gps_rows), len(can_rows))

    # print "GPS Rows: ", gps_rows
    print(datetime.now(), "GPS longitude: ", gps_rows[0]["longitude"],"---latitude: ", gps_rows[0]["latitude"])
    if min_data_rows_available > 1:
        # ---------------------------------------------------------------------------------------
        # | Send all the data to the server                                                     |
        # ---------------------------------------------------------------------------------------
        querryData = []
        for index in range(0, min_data_rows_available):
            # print "total: ", min_data_rows_available
            # print "index: ", index
            # print "lidar.length: ", len(lidar_rows)
            # print "lidar rows: ", lidar_rows[index]
            querryLine = {}
            querryLine['gyroscope_timestamp'] = 0
            querryLine['gyroscope_x_value'] = 0
            querryLine['gyroscope_y_value'] = 0
            querryLine['gyroscope_z_value'] = 0
            if index < len(accel_rows):
                querryLine['accelerometer_timestamp'] = str(accel_rows[index]['timestamp'])
                querryLine['accelerometer_x_value'] = accel_rows[index]['x']
                querryLine['accelerometer_y_value'] = accel_rows[index]['y']
                querryLine['accelerometer_z_value'] = accel_rows[index]['z']
            else:
                querryLine['accelerometer_timestamp'] = 0
                querryLine['accelerometer_x_value'] = 0
                querryLine['accelerometer_y_value'] = 0
                querryLine['accelerometer_z_value'] = 0

            if index < len(can_rows):
                querryLine['speed'] = can_rows[index]['speed']
                querryLine['rpm'] = can_rows[index]['rpm']
                querryLine['throttle_position'] = can_rows[index]['throttle_position']
                querryLine['engine_load'] = can_rows[index]['engine_load']
                querryLine['distance_travel_with_mil_on'] = can_rows[index]['distance_with_mil_on']
                querryLine['rtses'] = can_rows[index]['run_time_since_engine_start']
                querryLine['ftli'] = can_rows[index]['fuel_tank_level_input']
                querryLine['aapd'] = can_rows[index]['accelerator_pedal_position_d']
                querryLine['aape'] = can_rows[index]['accelerator_pedal_position_e']
                querryLine['aapf'] = can_rows[index]['accelerator_pedal_position_f']
                querryLine['ft'] = can_rows[index]['fuel_type']
                querryLine['engine_fuel_rate'] = can_rows[index]['engine_fuel_rate']
            else:
                querryLine['speed'] = 0
                querryLine['rpm'] = 0
                querryLine['throttle_position'] = 0
                querryLine['engine_load'] = 0
                querryLine['distance_travel_with_mil_on'] = 0
                querryLine['rtses'] = 0
                querryLine['ftli'] = 0
                querryLine['aapd'] = 0
                querryLine['aape'] = 0
                querryLine['aapf'] = 0
                querryLine['ft'] = 0
                querryLine['engine_fuel_rate'] = 0



            if index < len(lidar_rows):
                querryLine['lidar_timestamp'] = str(lidar_rows[index]['timestamp'])
                querryLine['lidar_front_distance_value'] = lidar_rows[index]['front_distance']
                querryLine['lidar_front_distance_unit'] = lidar_rows[index]['front_distance_measure_unit']
            else:
                querryLine['lidar_timestamp'] = 0
                querryLine['lidar_front_distance_value'] = 0
                querryLine['lidar_front_distance_unit'] = 'cm'

            if index < len(gps_rows):
                querryLine['gps_timestamp'] = str(gps_rows[index]['timestamp'])
                querryLine['tripId'] = gps_rows[index]['session_uuid']
                if gps_rows[index]['gps_time']:
                    querryLine['gps_satelite_utc_date'] = gps_rows[index]['gps_time']
                else:
                    querryLine['gps_satelite_utc_date'] = 'no data'
                querryLine['gps_longitude'] = gps_rows[index]['longitude']
                querryLine['gps_latitude'] = gps_rows[index]['latitude']
                if gps_rows[index]['speed'] == gps_rows[index]['speed']:
                    querryLine['gps_speed'] = gps_rows[index]['speed']
                else:
                    querryLine['gps_speed'] = 0
                querryLine['gps_altitude'] = gps_rows[index]['altitude']
                querryLine['gps_climb'] = gps_rows[index]['climb']
                querryLine['gps_track'] = gps_rows[index]['track']
                querryLine['gps_mode'] = gps_rows[index]['mode']

            querryLine['start'] = 1 if deviceJustStarted == 1 else 0
            querryData.append(querryLine)
            if deviceJustStarted == 1:
                deviceJustStarted = 2
        url = Constants.UPLOAD_REGULAR_DATA_SERVER_URL + '/' + Constants.DEVICE_SERIAL_NUMBER
        # print 'querry Data', querryData
        if printDebugData:
            print(datetime.now(), 'URL: ', url)
            print('last_querry_line: ', querryLine)
        
        try:
            res = requests.post(url, json=querryData)
            # print 'POST status code', res.status_code
            if printDebugData:
                print(datetime.now(), 'DATA POST json dump', res.content)
            canSendUploadRequest = True
            if res.status_code == 200:
                # ---------------------------------------------------------------------------------------
                # | mark the rows sent to server as read. In this way we know to not send them again to the server
                # ---------------------------------------------------------------------------------------
                deviceJustStarted = 0

                # mark sent accel rows as read
                #print "accel rows:", accel_rows
                if len(accel_rows) > 0:
                    id_list = [t['id'] for t in accel_rows]
                    accelerometerSensor.set_accell_database_values_as_read(database_connection, id_list)
                    if printDebugData:
                        print(datetime.now(), 'accel updated rows', (id_list))

                # mark sent lidar rows as read
                if len(lidar_rows) > 0:
                    id_list = [t['id'] for t in lidar_rows]
                    lidarSensor.set_lidar_database_values_as_read(database_connection, id_list)
                    if printDebugData:
                        print(datetime.now(), 'lidar updated rows', (id_list))

                # mark sent gps rows as read
                if len(gps_rows) > 0:
                    id_list = [t['id'] for t in gps_rows]
                    gpsSensor.set_gps_database_values_as_read(database_connection, id_list)
                    if printDebugData:
                        print(datetime.now(), 'gps updated rows', (id_list))

                # mark sent can rows as read
                if len(can_rows) > 0:
                    id_list = [t['id'] for t in can_rows]
                    set_can_database_values_as_read(database_connection, id_list)
                    if printDebugData:
                        print(datetime.now(), 'can updated rows', (id_list))

                if printDebugData:
                    print(datetime.now(), 'Succesfully uploaded ', min_data_rows_available, 'rows')
            else:
                print(datetime.now(), 'ERROR: There was an error uploading data: ', res.content)
        except ConnectionError as e:
            if deviceJustStarted == 2:
                deviceJustStarted = 1
            print(datetime.now(), 'UPLOAD ERROR: Connection error for url: ', Constants.UPLOAD_REGULAR_DATA_SERVER_URL + '/' + Constants.DEVICE_SERIAL_NUMBER)
            print(datetime.now(), 'UPLOAD ERROR: ', str(e))
            print(datetime.now(), 'Waiting ', cycleRun, ' seconds for connection...')
            # /////////////// 22.04.2021
            canSendUploadRequest = True
            # /////////////// 22.04.2021
    else:
        print(datetime.now(), 'ERROR: There is no data to upload right now: ', datetime.now())
        print(datetime.now(), 'gpsCount', len(gps_rows))


def pullSensorData(database_connection):
    # Read the inferred acceleration from accelerometer, gyroscope and magnetometer
    if accelerometerSensor:
        accelerometerSensor.get_accel_sensor_data()
    if gpsSensor:
        gpsSensor.get_gps_sensor_data()
    if lidarSensor:
        lidarSensor.get_lidar_sensor_data()

def insert_can_data_into_database(database_connection):
        sql_command = """
                          INSERT INTO
                             can
                             (session_uuid, 
                             speed, 
                             rpm, 
                             throttle_position, 
                             engine_load, 
                             distance_with_mil_on, 
                             run_time_since_engine_start, 
                             fuel_tank_level_input, 
                             accelerator_pedal_position_D,
                             accelerator_pedal_position_E, 
                             accelerator_pedal_position_F,
                             fuel_type,
                             driver_demand_engine, 
                             actual_demand_engine, 
                             fuel_system_control, 
                             engine_fuel_rate
                             )
                           VALUES
                             (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                       """
        try:
            session_uuid = Constants.Constants.getInstance().session_uuid
            cursor = database_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            speed_ = speed_queue.get()
            rpm_   = rpm_queue.get()
            throttle_position_ = throttle_position_queue.get()
            engine_load_ = engine_load_queue.get()
            distance_with_mil_on_ = distance_with_mil_on_queue.get()
            run_time_since_engine_start_ = run_time_since_engine_start_queue.get()
            fuel_tank_level_input_ = fuel_tank_level_input_queue.get()
            accelerator_pedal_position_D_ = accelerator_pedal_position_D_queue.get()
            accelerator_pedal_position_E_ = accelerator_pedal_position_E_queue.get()
            accelerator_pedal_position_F_ = accelerator_pedal_position_F_queue.get()
            fuel_type_ = fuel_type_queue.get()
            engine_fuel_rate_ = engine_fuel_rate_queue.get()
            driver_demand_engine_ = driver_demand_engine_queue.get()
            actual_demand_engine_ = actual_demand_engine_queue.get()
            fuel_system_control_ = fuel_system_control_queue.get()

            if not math.isnan(speed_) and not math.isnan(rpm_):
                cursor.execute(sql_command, (
                    str(session_uuid),
                    speed_,
                    rpm_,
                    throttle_position_,
                    engine_load_,
                    distance_with_mil_on_,
                    run_time_since_engine_start_,
                    fuel_tank_level_input_,
                    accelerator_pedal_position_D_,
                    accelerator_pedal_position_E_,
                    accelerator_pedal_position_F_,
                    fuel_type_,
                    driver_demand_engine_,
                    actual_demand_engine_,
                    fuel_system_control_,
                    engine_fuel_rate_
                ))
            else:
                cursor.execute(sql_command, (str(session_uuid), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, 0, 0))
            cursor.close()
        except Exception, e:
            print "CAN ERROR 2: error while inserting data in database: " + str(e)
            pass

def saveSensorDataInDatabase(database_connection):
    # save data in local DB at regular intervals
    print(datetime.now(), ' :Writing data into database every ', 1, 'seconds')
    # print datetime.now(), 'LIDAR: ', lidarSensor.distance
    if accelerometerSensor:
        accelerometerSensor.insert_accell_data_into_database(database_connection)
    else:
        print(datetime.now(), "SAVE DATA ERROR: no accelerometer object")
    if gpsSensor:
        gpsSensor.insert_gps_data_into_database(database_connection)
    else:
        print(datetime.now(), "SAVE DATA ERROR: no gps object")
    if lidarSensor:
        lidarSensor.insert_lidar_data_into_database(database_connection)
    else:
        print(datetime.now(), "SAVE DATA ERROR: no lidar object")
    if canSensor:
        insert_can_data_into_database(database_connection)
    else:
        print(datetime.now(), "SAVE DATA ERROR: no canSensor object")


def watchForEvents(database_connection):
    # test for events
    eventWatcher.search_for_driving_events_in_sensors(database_connection, accelerometerSensor, 0, lidarSensor, gpsSensor, canSensor)


if __name__ == "__main__":
    with psycopg2.connect("dbname=%s user=%s password=%s host=%s" % (Constants.DATABASE, Constants.USERNAME, Constants.PASSWORD, Constants.HOST)) as conn:
        # There is no need for transactions here, no risk of inconsistency etc
        conn.autocommit = True

        parse_calibration_file()

        accelerometerSensor = IMU.IMU.getInstance()
        lidarSensor = LidarModule.Lidar.getInstance()
        gpsSensor = GPSModule.GPS.getInstance()
        #canSensor = CANModule.CANModule.getInstance()
        canSensor = CANModule.CANModule(speed_queue,rpm_queue,throttle_position_queue,engine_load_queue,distance_with_mil_on_queue,accelerator_pedal_position_D_queue,accelerator_pedal_position_E_queue,accelerator_pedal_position_F_queue,fuel_type_queue,engine_fuel_rate_queue,driver_demand_engine_queue,actual_demand_engine_queue,fuel_system_control_queue)
        canSensor.start()
        eventWatcher = EventsWatcher.DrivingEvents()
        eventWatcher.send_event_to_server(conn)


        deviceJustStarted = 1
        run = True

        try:
            cycleRun = int(sys.argv[1])
            print(datetime.now(), 'START: Script will run every', cycleRun, 'seconds')
            printDebugData = int(sys.argv[2])

        except (ValueError, IndexError):
            cycleRun = Constants.SENSOR_DATA_PULLING_FREQUENCY
            print(datetime.now(), 'START: Script will run every', cycleRun, 'seconds')
            printDebugData = 1
        except Exception as e:
            print(datetime.now(), 'START: Script will run ??? ERROR' + str(e))

        while run:
            try:
                pullSensorData(conn)
                watchForEvents(conn)
                # saveSensorDataInDatabase(conn)

                if tick_count - int(math.floor(tick_count)) + 0.1 >= 1.0:
                    pass
                    saveSensorDataInDatabase(conn)
                    # DatabaseModule.insert_all_data_for_sensors(conn, accelerometerSensor, gpsSensor, lidarSensor, canSensor)

                if tick_count >= Constants.UPLOAD_DATA_FREQUENCY:
                    #/////////////// 22.04.2021
                    upload_thread_data = threading.Thread(target=uploadData, args=[conn])
                    upload_thread_data.start()
                    upload_thread_events = threading.Thread(target=eventWatcher.send_event_to_server, args=[conn])
                    upload_thread_events.start()
                    tick_count = 0
                    #/////////////// 22.04.2021

                tick_count = tick_count + cycleRun
                time.sleep(cycleRun)

            except KeyboardInterrupt:
                canSensor.disconnect_from_can()
                gpsSensor.gpsp.running = False
                gpsSensor.gpsp.join()
                run = False
                print(datetime.now(), 'ERROR: 1Closing database because of KeyboardInterrupt\n')

            except EOFError:
                print(datetime.now(), 'ERROR: 1Closing database because of EOFError\n')

            except ValueError:
                print(datetime.now(), 'ERROR: Value error')

            except InterfaceError:
                print(datetime.now(), 'ERROR: DB Error : Interface error')

            except Exception as e:
                canSensor.disconnect_from_can()
                gpsSensor.gpsp.running = False
                gpsSensor.gpsp.join()
                run = False
                print(datetime.now(), 'ERROR: Unknown error in SaveSensorData: ', str(e))
