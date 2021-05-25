import obd
from obd import OBDStatus
from obd import OBDResponse
import Constants
import math
import time
from multiprocessing import Process, Queue
import glob
from serial.serialutil import SerialException
import psycopg2
from datetime import datetime
obd.logger.setLevel(obd.logging.DEBUG) # enables all debug information

class CANModule(Process):
    __instance = None

    connection = 0

    speed = 0 #Kph | int
    rpm = 0 #rpm
    throttle_position = 0 #percent
    engine_load = 0 #percent
    distance_with_mil_on = 0 #Km
    run_time_since_engine_start = 0 #seconds
    fuel_tank_level_input = 0 #percent
    accelerator_pedal_position_D = 0 #percent
    accelerator_pedal_position_E = 0 #percent
    accelerator_pedal_position_F = 0 #percent
    fuel_type = "" #string
    engine_fuel_rate = 0 #l/h liters per hour

    driver_demand_engine = 0
    actual_demand_engine = 0
    fuel_system_control = 0

    verbose = 1

    speed_queue = []
    rpm_queue = []
    throttle_position_queue = []
    engine_load_queue = []
    distance_with_mil_on_queue = []
    run_time_since_engine_start_queue = []
    fuel_tank_level_input_queue = []
    accelerator_pedal_position_D_queue = []
    accelerator_pedal_position_E_queue = []
    accelerator_pedal_position_F_queue = []
    fuel_type_queue = []
    engine_fuel_rate_queue = []
    driver_demand_engine_queue = []
    actual_demand_engine_queue = []
    fuel_system_control_queue = []

    @staticmethod
    def getInstance():
        """Singleton. Only one instance will access the rfcomm port"""
        print "init singleton"
        if CANModule.__instance == None:
            CANModule()
        return CANModule.__instance

    def __init__(self, speed_q, rpm_q, throttle_position_q, engine_load_q, distance_with_mil_on_q, run_time_since_engine_start_q, fuel_tank_level_input_q, accelerator_pedal_position_D_q, accelerator_pedal_position_E_q, accelerator_pedal_position_F_q, fuel_type_q, engine_fuel_rate_q, driver_demand_engine_q, actual_demand_engine_q, fuel_system_control_q):
        """ Virtually private constructor. """
        print "CAN calling init"
        if CANModule.__instance != None:
            print "This class is a singleton CAN"
            raise Exception("This class is a singleton!")
        else:
            self.speed_queue = speed_q
            self.rpm_queue = rpm_q
            self.throttle_position_queue = throttle_position_q
            self.engine_load_queue = engine_load_q
            self.distance_with_mil_on_queue = distance_with_mil_on_q
            self.run_time_since_engine_start_queue = run_time_since_engine_start_q
            self.fuel_tank_level_input_queue = fuel_tank_level_input_q
            self.accelerator_pedal_position_D_queue = accelerator_pedal_position_D_q
            self.accelerator_pedal_position_E_queue = accelerator_pedal_position_E_q
            self.accelerator_pedal_position_F_queue = accelerator_pedal_position_F_q
            self.fuel_type_queue = fuel_type_q
            self.engine_fuel_rate_queue = engine_fuel_rate_q
            self.driver_demand_engine_queue = driver_demand_engine_q
            self.actual_demand_engine_queue = actual_demand_engine_q
            self.fuel_system_control_queue = fuel_system_control_q

            CANModule.__instance = self

    def disconnect_from_can(self):
        print 'CAN: Disconnectiong from CAN'
        if self.connection:
            if self.connection.running:
                self.connection.stop()
            self.connection.close()

            
    def connect_can_async(self):
        retry_number = 30
        self.connection = []
        self.connection = obd.Async(portstr='/dev/rfcomm0', baudrate=38400, protocol='6', timeout=5)
        while retry_number > 0:
            try:
                print "CAN 1, rety number: ", retry_number
                # OBD connection via Bluetooth (rfcomm port 0)
                possible_ports += glob.glob("/dev/rfcomm[0-9]*")
                for port in possible_ports:
                    if obd.try_port(port):
                        OBDports.append(port)

                if len(OBDports) == 0:
                    print "No OBD device or rfcomm is not created yet."
                    time.sleep(10)
                    raise SerialException

                OBDports.sort()
                print "found ports are " + OBDports
                self.connection = obd.Async(portstr='/dev/rfcomm0', baudrate=38400, protocol='6', timeout=5)

                print "CAN 2"
                if self.connection.status() == OBDStatus.CAR_CONNECTED:
                    print datetime.now(), "OBD Connected to CAN. CAR_CONNECTED!"
                    if self.connection.supports(obd.commands.RPM):
                        print datetime.now(), "Supports RPM!"
                        break
                    else:
                        print datetime.now(), "Doesn't support RPM!"
                        raise SerialException
                elif self.connection.status() == OBDStatus.OBD_CONNECTED:
                    print "OBD Connected to CAN. Ignition OFF!"
                    raise SerialException
                elif self.connection.status() == OBDStatus.ELM_CONNECTED:
                    print "OBD connected"
                    raise SerialException
                elif self.connection.status() == OBDStatus.NOT_CONNECTED:
                    print "OBD not connected"
                    raise SerialException

            except SerialException, e:
                print datetime.now(), str(e), 'retrying'
                retry_number -= 1
                self.disconnect_from_can()
                time.sleep(10)
                continue
            except KeyboardInterrupt:
                retry_number = 0
                self.disconnect_from_can()
            except Exception, e:
                print "CAN ERROR 1: " + str(e)
                retry_number -= 1
                time.sleep(10)
                continue

        # check wich protocols and sensor codes are available for the current vehicle
        print "CAN 3"
        if self.connection.is_connected():
            print 'PROTOCOL: ', self.connection.protocol_id(), ' - ', self.connection.protocol_name()
            for command in self.connection.supported_commands:
                if self.connection.supports(command):
                    print command

        if self.connection.is_connected():
            cmd = obd.commands.SPEED  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_speed_change, force=True)
            cmd = obd.commands.RPM  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_rpm_change, force=True)
            cmd = obd.commands.THROTTLE_POS  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_throttle_position_change, force=True)
            cmd = obd.commands.ENGINE_LOAD  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_engine_load_change, force=True)
            cmd = obd.commands.DISTANCE_W_MIL  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_mil_on_distance_change, force=True)
            cmd = obd.commands.RUN_TIME  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_run_time_since_engine_start_change, force=True)
            cmd = obd.commands.FUEL_LEVEL  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_fuel_tank_level_input_change, force=True)
            cmd = obd.commands.ACCELERATOR_POS_D  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_accelerator_pedal_position_D_change, force=True)
            cmd = obd.commands.ACCELERATOR_POS_E  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_accelerator_pedal_position_E_change, force=True)
            cmd = obd.commands.ACCELERATOR_POS_F  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_accelerator_pedal_position_F_change, force=True)
            cmd = obd.commands.FUEL_TYPE  # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_fuel_type_change, force=True)
            cmd = obd.commands.FUEL_RATE # select an OBD command (sensor)
            if self.connection.supports(cmd):
                self.connection.watch(cmd, callback=self.listen_for_engine_fuel_rate_change, force=True)
            # OBDCommand("EMISSION_REQ", "Driver's demand engine - percent torque", b"0161", 3, drop, ECU.ENGINE, True),
        self.connection.start()



    #THE FOLLOWING ARE USED ONLY WHEN PULLING THE DATA ASYNC
    # response_string = str(response.value)
    # quantity = response.value.magnitude
    # unit = response.value.units
    def listen_for_speed_change(self, response):
        if not response.is_null():
            if self.verbose:
                print "respone listen_for_speed_change: ", response
            if not response.is_null():
                self.speed = response.value.magnitude
                self.speed_queue.put(self.speed)

            # print 'SPEED:', self.speed
            # print 'RPM:', self.rpm
            # print 'THROTTLE:', self.throttle_position
            # print 'ENGINE_LOAD:', self.engine_load
            # print 'DISTANCE_W_MIL:', self.distance_with_mil_on
            # print 'RUN TIME SINCE ENGINE START:', self.run_time_since_engine_start
            # print 'FUEL TANK LEVEL INPUT:', self.fuel_tank_level_input
            # print 'ACCELERATOR PEDAL D:', self.accelerator_pedal_position_D
            # print 'ACCELERATOR PEDAL E:', self.accelerator_pedal_position_E
            # print 'ACCELERATOR PEDAL F:', self.accelerator_pedal_position_F
            # print 'FUEL TYPE:', self.fuel_type

    def listen_for_rpm_change(self, response):
        if self.verbose:
            print "respone listen_for_rpm_change: ", response
        if not response.is_null():
            self.rpm = response.value.magnitude
            self.rpm_queue.put(self.rpm)

    def listen_for_throttle_position_change(self, response):
        if self.verbose:
            print "respone listen_for_throttle_position_change: ", response
        if not response.is_null():
            self.throttle_position = response.value.magnitude
            self.throttle_position_queue.put(self.throttle_position)

    def listen_for_engine_load_change(self, response):
        if self.verbose:
            print "respone listen_for_engine_load_change: ", response
        if not response.is_null():
            self.engine_load = response.value.magnitude
            self.engine_load_queue.put(self.engine_load)

    def listen_for_mil_on_distance_change(self, response):
        if self.verbose:
            print "respone listen_for_mil_on_distance_change: ", response
        if not response.is_null():
            self.distance_with_mil_on = response.value.magnitude
            self.distance_with_mil_on_queue.put(self.distance_with_mil_on)

    def listen_for_run_time_since_engine_start_change(self, response):
        if self.verbose:
            print "respone listen_for_run_time_since_engine_start_change: ", response
        if not response.is_null():
            self.run_time_since_engine_start = response.value.magnitude
            self.run_time_since_engine_start_queue.put(self.run_time_since_engine_start)

    def listen_for_fuel_tank_level_input_change(self, response):
        if self.verbose:
            print "respone listen_for_fuel_tank_level_input_change: ", response
        if not response.is_null():
            self.fuel_tank_level_input = response.value.magnitude
            self.fuel_tank_level_input_queue.put(self.fuel_tank_level_input)

    def listen_for_accelerator_pedal_position_D_change(self, response):
        if self.verbose:
            print "respone listen_for_accelerator_pedal_position_D_change: ", response
        if not response.is_null():
            self.accelerator_pedal_position_D = response.value.magnitude
            self.accelerator_pedal_position_D_queue.put(self.accelerator_pedal_position_D)

    def listen_for_accelerator_pedal_position_E_change(self, response):
        if self.verbose:
            print "respone listen_for_accelerator_pedal_position_E_change: ", response
        if not response.is_null():
            self.accelerator_pedal_position_E = response.value.magnitude
            self.accelerator_pedal_position_E_queue.put(self.accelerator_pedal_position_E)

    def listen_for_accelerator_pedal_position_F_change(self, response):
        if self.verbose:
            print "respone listen_for_accelerator_pedal_position_F_change: ", response
        if not response.is_null():
            self.accelerator_pedal_position_F = response.value.magnitude
            self.accelerator_pedal_position_F_queue.put(self.accelerator_pedal_position_F)

    def listen_for_fuel_type_change(self, response):
        if self.verbose:
            print "respone listen_for_fuel_type_change: ", response
        if not response.is_null():
            self.fuel_type = response.value
            self.fuel_type_queue.put(self.fuel_type)

    def listen_for_driver_demand_engine_change(self, response):
        if self.verbose:
            print "respone listen_for_driver_demand_engine_change: ", response
        if not response.is_null():
            self.driver_demand_engine = response.value.magnitude

    def listen_for_actual_demand_engine_change(self, response):
        if self.verbose:
            print "respone listen_for_actual_demand_engine_change: ", response
        if not response.is_null():
            self.actual_demand_engine = response.value.magnitude

    def listen_for_fuel_system_control_change(self, response):
        if self.verbose:
            print "respone listen_for_fuel_system_control_change: ", response
        if not response.is_null():
            self.fuel_system_control = response.value.magnitude

    def listen_for_engine_fuel_rate_change(self, response):
        if self.verbose:
            print "respone listen_for_engine_fuel_rate_change: ", response
        if not response.is_null():
            self.engine_fuel_rate = response.value.magnitude
            self.engine_fuel_rate_queue.put(self.engine_fuel_rate)

    #STOP ASYNC FUNCT DECLARATION

