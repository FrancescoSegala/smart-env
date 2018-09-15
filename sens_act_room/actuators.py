
import os.path

_default_path = "actuators_room.txt"


############################# Actuators Class ##################################

class Actuator:
        'the count of all the actuators present in the room'
        all_actuators = 0

        def __init__(self, type, location, actuators_filename=_default_path):
            self.type = type
            self.location = location
            self.id = type+location+str(Actuator.all_actuators)
            if os.path.isfile(actuators_filename):
                lines = [line.rstrip('\n') for line in open(actuators_filename)]
                for line in lines:
                    curr_id = line.split(":")[0]
                    if curr_id == self.id :
                        print("Actuator "+self.id+" alreay exist. ERROR")
                        del self
                        return
                with open(actuators_filename, "a") as myfile:
                    #TODO fix this the default value of the actuator
                    myfile.write(self.id+":2.0\n")
            print("Created Actuator "+self.id)
            Actuator.all_actuators+=1

        def __del__(self):
            class_name = self.__class__.__name__
            #print( class_name, "destroyed")

        def get_value(self):
            # this get the current value of the Actuator
            return actual_actuators_level[self.type]

        def set_value(self, value):
            # this set the acutator value to value
            actual_actuators_level[self.type] = value

        def get_input(self, actuators_filename=_default_path):
            # this get an external input : a value in a file in the form Actuator_ID:value
            #since the actuator number is limited it will not affect the performances
            if os.path.isfile(actuators_filename):
                lines = [line.rstrip('\n') for line in open(actuators_filename)]
                for line in lines:
                    curr_id,value = line.split(":")
                    if curr_id == self.id :
                        if value != actual_actuators_level[self.type]:
                            self.set_value(value)

################################################################################