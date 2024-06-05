""" 
BBQ Consumer A
Name: Topaz Montague
Date: 6/4/24

File Description & Approach:
This Python script monitors "Food A" temperature by reading messages from a RabbitMQ 
queue and tracking recent readings with a deque. If the temperature change is minimal 
over 10 minutes, it sends an email alert using SMTP based on configurations from a TOML file. 
The script establishes a RabbitMQ connection, manages the queue, and processes messages 
with a callback function, ensuring robust error handling and graceful shutdown. 
It requires Python 3.11 for TOML support and includes a main function for 
direct execution and proper module importation.

"""

from collections import deque
from email.message import EmailMessage
import pika
import pprint
import smtplib
import sys
import tomllib  # requires Python 3.11

# Declare variables
foodA_temp_queue = "02-food-A"
foodA_deque = deque(maxlen=20)  # limited to 20 items (the 20 most recent readings)
subject_str = "FOOD A STALL"
content_str = "FOOD A STALL: Food A temp has changed by 1 degree or less in the last 10 minutes."

def CreateAndSendEmailAlert(email_subject: str, email_body: str):
    """Read outgoing email info from a TOML config file"""

    with open(".env.toml", "rb") as file_object:
        secret_dict = tomllib.load(file_object)
    pprint.pprint(secret_dict)

    # Basic information
    host = secret_dict["outgoing_email_host"]
    port = secret_dict["outgoing_email_port"]
    outemail = secret_dict["outgoing_email_address"]
    outpwd = secret_dict["outgoing_email_password"]

    # Create an instance of an EmailMessage
    msg = EmailMessage()
    msg["From"] = outemail
    msg["To"] = outemail
    msg["Reply-to"] = outemail
    msg["Subject"] = email_subject
    msg.set_content(email_body)

    print("========================================")
    print(f"Prepared Email Message: ")
    print("========================================")
    print()
    print(f"{str(msg)}")
    print("========================================")
    print()

    try:
        if port == 465:
            # Use SMTP_SSL for port 465
            server = smtplib.SMTP_SSL(host, port)
        else:
            # Use SMTP for port 587
            server = smtplib.SMTP(host, port)
            server.starttls()

        server.set_debuglevel(2)

        print("========================================")
        print(f"SMTP server created: {str(server)}")
        print("========================================")
        print()

        server.login(outemail, outpwd)
        print("========================================")
        print(f"Successfully logged in as {outemail}.")
        print("========================================")
        print()

        server.send_message(msg)
        print("========================================")
        print(f"Message sent.")
        print("========================================")
        print()
    except Exception as e:
        print(f"Failed to connect or send email: {e}")
    finally:
        server.quit()
        print("========================================")
        print(f"Session terminated.")
        print("========================================")

def foodA_callback(ch, method, properties, body):
    """Define behavior on getting a message about the temperature of food A"""
    # Define a list to place food A temps initializing with 0
    foodAtemp = ['0']
    # Split timestamp and temp
    message = body.decode().split(",")
    # Assign the temp to a variable and convert to float
    foodAtemp[0] = float(message[-1])
    # Add the temp to the deque
    foodA_deque.append(foodAtemp[0])
    # Check to see that the deque has 20 items before analyzing
    if len(foodA_deque) == 20:
        # Assign difference in most recent temp and oldest temp in deque to a variable as a float
        foodA_temp_check = round(float(foodA_deque[-1] - foodA_deque[0]), 1)
        # If the temp has changed by 1 degree or less in 10 minutes, then an alert is sent
        if foodA_temp_check <= 1:
            print("FOOD STALL: Current food A temp is:", foodAtemp[0], ";", "Food A temp change in last 10 minutes is:", foodA_temp_check, "degrees")
            # Send user an email alert
            CreateAndSendEmailAlert(subject_str, content_str)
        # Let user know current temp
        else:
            print("Current food A temp is:", foodAtemp[0])
    else:
        # If the deque has less than 20 items the current temp is printed
        print("Current food A temp is:", foodAtemp[0])
    # Acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue.
    
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        foodA_temp_queue (str): food A queue
    """
    try:
        # Try this code, if it works, keep going
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()
        # Use the channel to clear the queue
        channel.queue_delete(foodA_temp_queue)
        # Use the channel to declare a durable queue
        channel.queue_declare(foodA_temp_queue, durable=True)
        # Set the prefetch count to one to limit the number of messages being consumed and processed concurrently
        channel.basic_qos(prefetch_count=1)
        # Configure the channel to listen on a specific queue and use the foodA_callback function
        channel.basic_consume(foodA_temp_queue, auto_ack=False, on_message_callback=foodA_callback)
        # Print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")
        # Start consuming messages via the communication channel
        channel.start_consuming()
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

if __name__ == "__main__":
    main("localhost", "foodA_temp_queue")
