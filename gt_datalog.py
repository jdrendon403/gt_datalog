import requests
import pymysql
import json
import time

read_error = False # Variable that store if there is an error with the PLC comunication


def read_data(command): #Function to read data from the PLC
    url = "http://172.53.5.2:8001/GTS/gen" # Webservice URL. this is the PLC's webservice
    data = {"command":{"command":"send_vessels"}} #Basic command structure to consume the PLC webservice
    data["command"]["command"] = command #Over write the command with the one in the function argument

    data = requests.post(url, json=data) #consume the webservice
    data = data.json() # Extract the data from the webservis respone
    data = json.dumps(data) #Convert the data to a json string

    # print(type(data))
    return data

def write_db():
    global read_error
    try: #Try to connect to the data base
        connection = pymysql.connect(
        host="172.16.2.73",
        user="jdrendon",
        password="853211",
        database="ionheat"
        )
        with connection: #If the connection is succes assmble the insert query
            with connection.cursor() as cursor:
                try:
                    sql = f"""INSERT INTO `datalog`(`vessel`, `power_supply`, `gas_flow` )
                    VALUES ('{read_data("send_vessels")}', 
                    '{read_data("send_power_supply")}', 
                    '{read_data("send_gas_flow")}');""" #Use the read function to read several data from the PLC
                    # print(sql)
                except: #In the case that the communication with the PLC fails 
                    try:
                        if not read_error:
                            read_error = True # tell the system that the communiation is in error
                            sql = f"""INSERT INTO eventlog (description)
                            VALUES ('read data error')"""
                            cursor.execute(sql) # Insert an entry in the even log
                    except:
                        print("event insert error") #In the case that it can't inset the event print a messsage in the terminal
                else: #if the PLC communication is succesful 
                    try:
                        cursor.execute(sql) #Insert the data
                        if read_error: #if the communication with the plc was in error 
                            sql = f"""INSERT INTO eventlog (description)
                            VALUES ('read data OK')"""
                            cursor.execute(sql) #Insert en event log indication that the communication is back
                        read_error = False # tell the system that the communiation is OK
                    except:
                        print("Data insert error") #In the case that it can't inset the event print a messsage in the terminal
                
                connection.commit() # Save the data inserted in the Data Base
                # print(result)
                # print("Data was saved")

        
    except: #If no succselful connection with the data base 
        print("DataBase connection error") # print a messsage in the terminal

def read_db(): # This function is to check if the database insert is working and only read the last row inserted
    connection = pymysql.connect(
    host="172.16.2.73",
    user="jdrendon",
    password="853211",
    database="ionheat"
    )

    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM datalog LIMIT 10"
            cursor.execute(sql)
            result = cursor.fetchall()
            print(result)

def main(): 
    while True:
        write_db() # read from plc (inside the function) and write to the data base
        time.sleep(60) #every 60 seconds

if __name__ == "__main__":
    # write_db()
    # read_db()
    # print(read_data("send_power_supply"))
    read_error = False
    main()