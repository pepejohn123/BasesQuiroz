#!/usr/bin/env python3
import os

import pydgraph

import model
import csv_clean
import json_creator
import splitter

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def print_menu():
    mm_options = {
        0: "Clean CSV!(you need a flight_passengers.csv)",
        1: "Create data",
        2: "Search for the 5 airlines with the lowest wait time",
        3: "Search for the 5 airlines with the highest wait time",
        6: "Delete airline",
        7: "Drop All",
        8: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)
    half1 = "half1_flights"
    half2 = "half2_flights"
    # Create schema
    model.set_schema(client)

    while(True):
        
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 0:
            csv_clean.modify_and_write_csv()
            json_path = json_creator.count_flights_per_airline()
            splitter.split_json(json_path, half1, half2)
        if option == 1:
            model.create_data(client,half1)
            model.create_data(client, half2)
        if option == 2:
            model.fastest_airlines(client )
        if option == 3:
            model.slowest_airlines(client )

        if option == 6:
            name = input("Name: ")
            model.delete_artist_song(client, name)
        if option == 7:
            model.drop_all(client)
        if option == 8:
            model.drop_all(client)
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))