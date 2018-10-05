
default_values_a = {"air":5.0 , "temp":20.0 , "light":1.6 }


############################# Actuators Class ##################################

class Actuator:
        'the count of all the actuators present in the room'
        all_actuators = 0
        actuators_level = {}


        def __init__(self, type, location , kappa ):
            self.type = type
            self.location = location
            self.id = type+location+str(Actuator.all_actuators)
            self.kappa = kappa
            if self.id in list(Actuator.actuators_level.keys()):
                print("Actuator ",self.id, " alreasy exist")
                del self
            Actuator.actuators_level[self.id] = default_values_a[self.type]
            print("Created Actuator "+self.id)
            Actuator.all_actuators+=1

        def __del__(self):
            class_name = self.__class__.__name__
            #print( class_name, "destroyed")

        def get_value(self):
            # this get the current value of the Actuator
            return Actuator.actuators_level[self.id]

        def set_value(self, value):
            # this set the acutator value to value
            Actuator.actuators_level[self.id] = value



################################################################################
