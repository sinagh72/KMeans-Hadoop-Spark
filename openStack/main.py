from flask import Flask
from openstack import connection
from flask import request
from datetime import datetime
import threading
import sched, time
from flask import jsonify

"""REST API
payload = {
    start_peak: yyyy/mm/dd HH:MM:SS,
    end_peak:   yyyy/mm/dd HH:MM:SS,
    vm_numbers: ,
    flavor: ,
    image: ,

}
"""

app = Flask(__name__)
data = {}#data dictionary
conn = None#connection obj
scheduler = None#scheduler 

@app.errorhandler(400)
@app.route("/scheduled", methods=['POST'])
def server_handler():
    global scheduler
    try:
        if not request.is_json:
            return jsonify(error=str("payload must be josn")), 400
        content = request.get_json()
        #checing if the start < end
        if (datetime.strptime(content['start_peak'], '%Y/%m/%d %H:%M:%S') - datetime.strptime(content['end_peak'], '%Y/%m/%d %H:%M:%S')).total_seconds() > 0:
            return jsonify(error=str("start peak must be less than end peak")), 400
        #set the starter
        a = time_handler(content=content, key='start_peak', func=create_server)
        #set the end
        b = time_handler(content=content, key='end_peak', func=delete_server, priority=2)

        if a and b:
            t = threading.Thread(target=scheduler.run)
            t.start()
            return "done!"
        else:
            return jsonify(error=str("The input time has already passed")), 400
    except:
        return jsonify(error=str( "Wrong input format\npayload = {\nstart_peak: yyyy/mm/dd HH:MM:SS,\nend_peak:   yyyy/mm/dd HH:MM:SS,\nvm_numbers: int,\nflavor: str, \nimage: str\n}")), 400
    return "True"

def create_server(content):
    global data
    global conn

    for i in range(0,content["vm_numbers"]):
        server = conn.compute.create_server(name="scheduled_vm"+str(i)+content['start_peak']+content['end_peak'], image_id=data[content["image"]+"-img"], flavor_id=data[content["flavor"]+"-flv"],
        networks=[{"uuid": data["internal-net"]}])
        data["scheduled_vm"+str(i)+content['start_peak']+content['end_peak']+'-srv'] = server.id
        server = conn.compute.wait_for_server(server)
    

def delete_server(content):
    global data
    global conn
    for i in range(0,content["vm_numbers"]):
        conn.compute.delete_server(data["scheduled_vm"+str(i)+content['start_peak']+content['end_peak']+'-srv'])
    
def time_handler(content, key, func, priority=1):
    global scheduler
    global data
    run_time_delay = (datetime.strptime(content[key], '%Y/%m/%d %H:%M:%S') - datetime.utcnow()).total_seconds()
    if run_time_delay < 0:
        return False
    scheduler.enter(run_time_delay, priority, func, (content,))
    return True

def init():
    global scheduler
    global data
    global conn
    scheduler = sched.scheduler(time.time, time.sleep)
    auth_args = {
        'auth_url':'http://252.3.51.106:5000/v3',
        'project_name':'admin',
        'domain_name' : 'admin_domain',
        'username':'admin',
        'password':'openstack',
    }
    conn = connection.Connection(**auth_args)
    for server in conn.compute.servers():
        data[server.name+'-srv'] = server.id
    for image in conn.compute.images():
        data[image.name+'-img'] = image.id
    for flavor in conn.compute.flavors():
        data[flavor.name+'-flv'] = flavor.id
    for network in conn.network.networks():
        data[network.name+'-net'] = network.id

    if 'standard-flv' not in data:
        flv = conn.compute.create_flavor(name='standard',ram=128, vcpus=1, disk=1)
        data[flv.name+'-flv'] = flv.id
    if 'large-flv' not in data:
        flv = conn.compute.create_flavor(name='large',ram=256, vcpus=2, disk=1)
        data[flv.name+'-flv'] = flv.id
    
    return conn, data, scheduler

if __name__ == '__main__':
    init()
    app.run(debug=True, host='0.0.0.0')

