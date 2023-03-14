import docker
import sys
import os
import json
import itertools
from iteration_utilities import random_product
import random
import hashlib
from time import sleep
import shutil

TESTING_COMPETITORS = ["arbitrage_autotrader", "competition_autotrader"]
MAX_NUMBER_PARAMETER_COMBINATIONS = 1 

# argument for docker run:
# docker run -v "/$(pwd)/ready_trader_go:/ready_trader_go" -v "/$(pwd)/test_dir:/gen_data"  python-test optiver_trader.sha256

def get_next_parameter_combination(parameters_file : str):
    if not os.path.exists(parameters_file):
        print("Path doesn't exist", parameters_file)
        return {}

    with open(parameters_file, "r") as file:
        parameter_options = json.load(file)

        parameter_names = []
        permutable_parameter_values = []
        number_permutations = 1

        for parameter_name, param_options in parameter_options.items():
            parameter_names.append(parameter_name)
            permutable_parameter_values.append(param_options)
            number_permutations *= len(param_options)

        # We'll test at most 128 different combinations of the parameters
        number_permutations = min(number_permutations, MAX_NUMBER_PARAMETER_COMBINATIONS)

        permutations = random_product(*permutable_parameter_values, repeat=number_permutations)[:number_permutations * len(parameter_names)] 
        print(permutations)

        for i in range(len(permutations))[::len(parameter_names)]:
            obj = {}
            for j in range(len(parameter_names)):
                obj[parameter_names[j]] = permutations[i + j]
            yield obj

def get_mounts():
    user_working_dir = os.getcwd()

    # mount target dir needs to be absolute
    ready_trader_go_code = docker.types.Mount(target="/pyready_trader_go/ready_trader_go",
                                        source=user_working_dir+"/ready_trader_go",
                                        type="bind")

    trading_strategies = docker.types.Mount(target="/pyready_trader_go/traders",
                                      source=user_working_dir+"/traders",
                                      type="bind")

    mounts = [ready_trader_go_code, trading_strategies]

    return mounts 

def run_benchmark(trader_name):
    config_file = os.path.join("traders", trader_name, trader_name + ".json")
    parameters_file = os.path.join("traders", trader_name, "testing_parameters.json") 

    if trader_name is None or len(trader_name) == 0 or not os.path.exists(config_file):
        print("Trader doesn't exist!")
        print("Please create a folder for the trading strategy that will be tested")
        exit(1)

    trader_config = json.load(open(config_file, "r"))

    client = docker.from_env()
    mounts = get_mounts()

    trader_code_hash = ""
    with open(os.path.join("traders", trader_name, trader_name + ".py")) as file:
        trader_code_hash = hashlib.sha256(file.read().encode()).hexdigest()

    for parameters in get_next_parameter_combination(parameters_file): 
        trader_config["Parameters"] = parameters
        hex_code = hashlib.sha256((trader_code_hash + str(parameters)).encode()).hexdigest()[:6]
        print("Running container with hash {0} and parameters {1}".format(hex_code, parameters))
        config_path = os.path.join("traders", trader_name, "{0}.{1}.json".format(trader_name, hex_code))
        json.dump(trader_config, open(config_path, "w"), indent=4)

        container = client.containers.run(image="trader-tester",
                            mounts=mounts,
                            auto_remove=True,
                            name="{0}.{1}".format(trader_name, hex_code),
                            command="{0}.{1}".format(trader_name, hex_code),
                            detach=False,
                            remove=True)

        os.remove(config_path) 


if __name__ == "__main__":
    trader_name = sys.argv[1]
    run_benchmark(trader_name)
    