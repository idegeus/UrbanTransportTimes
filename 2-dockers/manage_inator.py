from flask import Flask
import docker
import threading
import time

# === Controller methods ===

def run_docker():
    """ This function run a Docker container in detached mode and waits for completion. 
        Afterwards, it performs some tasks based on container's result.
    """

    # docker_client = docker.from_env()
    # container = docker_client.containers.run(image='bfirsh/reticulate-splines', detach=True)

    # result = container.wait()
    # container.remove()

    # if result['StatusCode'] == 0:
    #     do_something()  # For example access and save container's log in a json file
    # else:
    #     print('Error. Container exited with status code', result['StatusCode'])

def run_docker_2():
    print("Gezellig")
    time.sleep(2)
    print("Gezelliger!")

def main():
    """ This function start a thread to run the docker container and 
        immediately performs other tasks, while the thread runs in background.
    """

    threading.Thread(target=run_docker_2, name='run-docker').start()
    # Perform other tasks while the thread is running.

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/health")
def health():
    return {'status': 200, "response": "Manger server is operational."}

@app.route("/callback/osm_inator")
def osm_inator_callback():
    return {}

