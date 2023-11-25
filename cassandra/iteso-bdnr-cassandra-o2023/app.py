#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'flights')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Show FLIGHTS",
        2: "Show potential clients",
        3: "Show potential + actual clients",
        4: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)


    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            filter_choice = int(input('Do you want to filter the data? (0: no, 1: yes): '))
            if(filter_choice)==0:
                model.get_flights(session)
            if(filter_choice)==1:
                month_choice = int(input('Which month? 1-12 range (enter 0 for every flight): '))
                reason_options = {
                    1: "On vacation/Pleasure",
                    2: "Business/Work",
                    3: "Back Home",
                }
                for key in reason_options.keys():
                    print(key, '--', reason_options[key])
                reason = int(input('Which reason?: '))
                reason = reason_options[reason]


                stay_options = {
                    1: "Hotel",
                    2: "Short-term homestay",
                    3: "Home",
                    4: "Friend/Family",
                }
                for key in stay_options.keys():
                    print(key, '--', stay_options[key])
                stay = int(input('Which stay?: '))
                stay = stay_options[stay]

                transit_options = {
                    1: "Airport cab",
                    2: "Car rental",
                    3: "Mobility as a service",
                    4: "Public Transportation",
                    5: "Pickup",
                    6: "Own car",
                }
                for key in transit_options.keys():
                    print(key, '--', transit_options[key])
                transit = int(input('Which transit?: '))
                transit = transit_options[transit]
                args = [month_choice, reason,stay, transit]
                model.get_flights_by_month(session,args)       
        if option == 2:
            option = int(input('Single airport info, or every airport? \n(0 Single, 1All): '))
            if(option==0):
                airport = input('Enter your airport:(0 to see the best month for each airport)')
                model.get_actual_clients(session,airport)
            else:
                model.biggest_ratio(session, 0)    
        if option == 3:
            option = int(input('Single airport info, or every airport? \n(0 Single, 1All): '))
            if(option==0):
                airport = input('Enter your airport:(0 to see the best month for each airport)')
                model.get_actual_clients(session,airport)
            else:
                model.biggest_ratio(session, 1)    
        if option == 4:
            exit(0)
if __name__ == '__main__':
    main()
