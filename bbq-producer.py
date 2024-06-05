""" 
BBQ Producer
Name: Topaz Montague
Date: 6/4/24

File Description & Approach:
This Python script reads temperature data from a CSV file and sends it to three RabbitMQ task queues, 
simulating real-time temperature readings from a smart smoker and two food items. It establishes a RabbitMQ connection, 
clears any existing messages in the queues, and declares durable queues for message persistence. 
The script iterates through the CSV file, converts temperature readings to floats, formats them as messages, 
and publishes them to the respective queues with a 30-second delay between readings. 
Error handling ensures graceful exit if the RabbitMQ connection fails, and the connection is closed after processing

"""

import csv
import pika
import sys
import time
import webbrowser

# Declare variables
smoker_temp_queue = "01-smoker"
foodA_temp_queue = "02-food-A"
foodB_temp_queue = "03-food-B"
csv_file = 'smoker-temps.csv'

def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website."""
    if show_offer == "True":
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()
    else:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def main(host: str, csv_file: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        csv_file (str): the CSV file to read data from
    """
    try:
        # Read CSV file
        with open(csv_file, 'r') as file:
            reader = csv.reader(file, delimiter=',')
            # Skip header
            header = next(reader)

            try:
                # Create a blocking connection to the RabbitMQ server
                conn = pika.BlockingConnection(pika.ConnectionParameters(host))
                # Use the connection to create a communication channel
                ch = conn.channel()

                # Clear queues to clear out old messages
                ch.queue_delete(smoker_temp_queue)
                ch.queue_delete(foodA_temp_queue)
                ch.queue_delete(foodB_temp_queue)

                # Declare durable queues
                ch.queue_declare(smoker_temp_queue, durable=True)
                ch.queue_declare(foodA_temp_queue, durable=True)
                ch.queue_declare(foodB_temp_queue, durable=True)

                for row in reader:
                    Time, Channel1, Channel2, Channel3 = row

                    # Convert numbers to floats and send messages to respective queues
                    try:
                        smoker_temp = float(Channel1)
                        smoker_message = f"{Time}, {smoker_temp}".encode()
                        ch.basic_publish(exchange="", routing_key=smoker_temp_queue, body=smoker_message)
                        print(f" [x] Sent {smoker_message} on {smoker_temp_queue}")
                    except ValueError:
                        pass

                    try:
                        foodA_temp = float(Channel2)
                        foodA_message = f"{Time}, {foodA_temp}".encode()
                        ch.basic_publish(exchange="", routing_key=foodA_temp_queue, body=foodA_message)
                        print(f" [x] Sent {foodA_message} on {foodA_temp_queue}")
                    except ValueError:
                        pass

                    try:
                        foodB_temp = float(Channel3)
                        foodB_message = f"{Time}, {foodB_temp}".encode()
                        ch.basic_publish(exchange="", routing_key=foodB_temp_queue, body=foodB_message)
                        print(f" [x] Sent {foodB_message} on {foodB_temp_queue}")
                    except ValueError:
                        pass

                    # Read values every second
                    time.sleep(30)

            except pika.exceptions.AMQPConnectionError as e:
                print(f"Error: Connection to RabbitMQ server failed: {e}")
                sys.exit(30)
            finally:
                # Close the connection to the server
                conn.close()

    except FileNotFoundError as e:
        print(f"Error: CSV file not found: {e}")
        sys.exit(30)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(30)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions without executing the code below.
if __name__ == "__main__":
    offer_rabbitmq_admin_site("False")
    main("localhost", csv_file)