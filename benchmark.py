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
import pathlib
import openpyxl

TESTING_COMPETITORS = ["arbitrage_autotrader", "optiver_trader"]
MAX_NUMBER_PARAMETER_COMBINATIONS = 20 
MAX_CONCURRENT_SIMULATIONS = 10 

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

        permutations = random_product(*permutable_parameter_values, repeat=4 * number_permutations)

        for i in range(len(permutations))[::len(parameter_names)]:
            obj = {}
            for j in range(len(parameter_names)):
                obj[parameter_names[j]] = permutations[i + j]
            yield obj

    yield None

def create_report(score_board_files):
    open("benchmark_report.xlsx", "a").close()

    wb = openpyxl.load_workbook("benchmark_report.xlsx")

    for file in score_board_files:
        



def run_batch_of_containers(client, mounts, containers_info):
    running_containers = []

    for trader_name, hex_code, command, config in containers_info:
        print("Running container {0}.{1} testing {0} with parameters {2}".format(trader_name, hex_code, config))
        running_containers.append(client.containers.run(image="trader-test",
                                mounts=mounts,
                                name="{0}.{1}".format(trader_name, hex_code),
                                command=command,
                                detach=True))

    while len(running_containers) > 0:
        containers_to_remove = []

        for i in range(len(running_containers)):
            container = client.containers.get(running_containers[i].id)
            container.reload()

            dkg = container.logs(stream = True, follow = False)
            try:
                while True:
                    line = next(dkg).decode("utf-8")
                    print("[{0}]".format(container.name), line)
            except StopIteration:
                # print(f'Log stream ended for {container.name}')   
                pass

            status = client.containers.get(running_containers[i].id).status
            if status == "exited" or status == "removed":
                containers_to_remove.append(i)

        for index in containers_to_remove[::-1]:
            container = client.containers.get(running_containers[index].id)
            container.remove()
            running_containers.pop(index)

        sleep(1)

    print("Finished running batch of containers")


def get_mounts():
    user_working_dir = os.getcwd()

    # mount target dir needs to be absolute
    ready_trader_go_code = docker.types.Mount(target="/pyready_trader_go/ready_trader_go",
                                        source=user_working_dir+"/ready_trader_go",
                                        type="bind")

    trading_strategies = docker.types.Mount(target="/pyready_trader_go/traders",
                                      source=user_working_dir+"/traders",
                                      type="bind")

    rtg_file = docker.types.Mount(target="/pyready_trader_go/rtg.py",
                                  source=user_working_dir+"/rtg.py",
                                  type="bind")

    exchange_file = docker.types.Mount(target="/pyready_trader_go/exchange.json",
                                  source=user_working_dir+"/exchange.json",
                                  type="bind")

    mounts = [ready_trader_go_code, trading_strategies, rtg_file, exchange_file]

    return mounts 

def run_benchmark(trader_name):
    config_file = os.path.join("traders", trader_name, trader_name + ".json")
    parameters_file = os.path.join("traders", trader_name, "testing_parameters.json") 

    if trader_name is None or len(trader_name) == 0 or not os.path.exists(config_file):
        print("Trader doesn't exist!")
        print("Please create a folder for the trading strategy that will be tested")
        exit(1)

    prev_log_folders = os.listdir(os.path.join("traders", trader_name))

    trader_config = json.load(open(config_file, "r"))

    client = docker.from_env()
    mounts = get_mounts()

    trader_code_hash = ""
    with open(os.path.join("traders", trader_name, trader_name + ".py")) as file:
        trader_code_hash = hashlib.sha256(file.read().encode()).hexdigest()

    tried_combinations = []
    generator = get_next_parameter_combination(parameters_file)
    testing_done = False

    while not testing_done:
        next_batch_of_containers = []
        
        while not testing_done and len(next_batch_of_containers) < MAX_CONCURRENT_SIMULATIONS:
            parameters = next(generator)

            if parameters == None:
                # no more parameter combinations to try, we're done
                testing_done = True
            else:
                trader_config["Parameters"] = parameters
                hex_code = hashlib.sha256((trader_code_hash + str(parameters)).encode()).hexdigest()[:6]

                # we don't want to repeat a simulation with parameters we've already tried
                if hex_code in tried_combinations:
                    continue

                tried_combinations.append(hex_code)

                config_path = os.path.join("traders", trader_name, "{0}.{1}.json".format(trader_name, hex_code))
                json.dump(trader_config, open(config_path, "w"), indent=4)

                command = "{0}.{1}".format(trader_name, hex_code)
                for competitor_name in TESTING_COMPETITORS:
                    command += " " + competitor_name

                # TODO: Add competitors
                next_batch_of_containers.append((trader_name, hex_code, command, str(trader_config)))

        if len(next_batch_of_containers) == 0:
            assert testing_done == True
            continue
            
        run_batch_of_containers(client, mounts, next_batch_of_containers)

        if len(tried_combinations) > MAX_NUMBER_PARAMETER_COMBINATIONS:
            testing_done = True

    folders_now = os.listdir(os.path.join("traders", trader_name))
    simulation_scores = []

    for folder in folders_now:
        if os.path.isdir(folder) and not folder in prev_log_folders:
            simulation_scores.append(os.path.join("traders", trader_name, folder, "score_board.csv"))

    create_report(simulation_scores)


if __name__ == "__main__":
    trader_name = sys.argv[1]
    run_benchmark(trader_name)
    