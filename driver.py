
from log4python.Log4python import log
import fire
#import logging
#import logging.config

DemoLogger = log("LogDemo")
RootLogger = log("root")

RootLogger.error("this is a dummy error in Root")
DemoLogger.error("this is a error in Demo")

class DriverProgram:
    #logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self,fileType):
        RootLogger.debug("I am within the constructor")
        self.file_type = fileType
    def my_function(self):
        RootLogger.debug("inside my function . Processing "+self.file_type + " file")

def print_hi(name):
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

if __name__ == '__main__':
    print("Entering the main method")
    print_hi('PyCharm')
    driver = DriverProgram("csv")
    driver.my_function()


