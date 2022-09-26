import requests
import pymysql
import json
import time

read_error = False # Variable that store if there is an error with the PLC comunication


def read_data(command):
    url = "http://172.16.1.19:8001/GTS/gen" # Webservice URL. this is the PLC's webservice
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
                            read_error = True
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
                            cursor.execute(sql)
                        read_error = False
                    except:
                        print("Data insert error")
                
                connection.commit()
                # print(result)
                print("Data was saved")

        
    except:
        print("DataBase connection error")

def read_db():
    connection = pymysql.connect(
    host="172.16.2.73",
    user="jdrendon",
    password="853211",
    database="ionheat"
    )

    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM datalog ORDER BY id DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchall()
            print(result)

def main():
    while True:
        write_db()
        time.sleep(60)

if __name__ == "__main__":
    # write_db()
    read_db()
    # print(read_data("send_power_supply"))
    # read_error = False
    # main()