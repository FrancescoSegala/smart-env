#!/usr/bin/env python
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys




 #default path for the actuators file
_default_path = "actuators_room.txt"

########################### Updating function ##################################
def replace_value(value):
    #TODO get lock here
    target_id = value.split(":")[0]
    out = open(_default_path,"w")
    for line in open(_default_path, "r"):
        curr_id = line.split(":")[0]
        if curr_id == target_id :
            print("written: "+value)
            out.write(value+"\n")
        else :
            out.write(line)
    out.close()
    #TODO release lock here

################################################################################

# HTTPRequestHandler class
class actuatorHTTPrequestHandler(BaseHTTPRequestHandler):

    # GET
    def do_GET(self):
        # Send response status code

        self.send_response(200)
        # Send headers
        self.send_header('Content-type','text/html')
        self.end_headers()
        path = self.path.split("/")
        if len(path) < 3 :
            self.wfile.write(bytes("invalid format", "utf8"))
            return
        id = path[1]
        value = path[2]
        # /replace_value(id+":"+value)
        print("actuator "+id+" value changed to "+value)
        # Send message back to client
        message = "value Updated"
        # Write content as utf-8 data
        self.wfile.write(bytes(message, "utf8"))
        return

################################################################################



def http_usage():
    print("usage: python3 actuator_receiver.py -p #port")


def run(port):
  print('starting server...')
  # Choose port 8080, for port 80, which is normally used for a http server, you need root access
  server_address = ('127.0.0.1', port)
  httpd = HTTPServer(server_address, actuatorHTTPrequestHandler)
  print('running server...')
  httpd.serve_forever()

############################ Loader method #####################################



def main():
    if len(sys.argv) != 3 :
        http_usage()
        return
    port = 8080
    if sys.argv[1] == "-p" :
        port = int(sys.argv[2])
    else :
        http_usage()
        return
    run(port)

if __name__ == "__main__":
    main()


################################EOF#############################################
