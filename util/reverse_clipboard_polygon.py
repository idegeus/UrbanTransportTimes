import subprocess
import json
import flexpolyline

p = subprocess.Popen(['pbpaste'], stdout=subprocess.PIPE)
data = json.loads(p.stdout.read())

if "outer" in data[0]:
    data = flexpolyline.decode(data[0]['outer'])

if isinstance(data[0], dict) and "coordinates" in data[0]:
    data = [e['coordinates'][0] for e in data]

if isinstance(data[0][0], float):
    data = [data]

data = [[[e[1],e[0]] for e in f] for f in data] 
subprocess.run("pbcopy", text=True, input=str(data))

print("Done!")
