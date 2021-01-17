import json
import logging
import pika
import sys
from db_manager import DbManager
from repo_scanner import RepoScanner
from config import INPUT_QUEUE, OUTPUT_QUEUES


class Messenger:
    """Handles input and output messages in RabbitMQ message-broker.
    Calls other classes' methods for extracting repository."""

    # Input channel
    _input_channel = None

    # Output channels identified by their language names
    _output_channels = dict()

    @staticmethod
    def app(rabbitmq_host, rabbitmq_port):
        """Main method of the app. Makes connection to RabbitMQ, initializes channels,
        performs loop for handling input messages"""
        while True:
            try:
                # Make connection to RabbitMQ
                logging.info(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

                Messenger._create_channels(connection)
                logging.info("Connected.")
                while True:
                    Messenger._input_channel.basic_consume(
                        queue=INPUT_QUEUE,
                        auto_ack=False,
                        on_message_callback=Messenger._consume_input)
                    logging.info(' [*] Waiting for a message about extracting a new repo...')
                    Messenger._input_channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as exception:
                logging.error("AMQP Connection Error: {}".format(exception))
            except KeyboardInterrupt:
                logging.info(" Exiting...")
                try:
                    connection.close()
                except NameError:
                    pass
                return

    @staticmethod
    def _create_channels(connection):
        """Creates input and output channels"""
        logging.info("Initializing input & output channels...")
        # Create input channel
        Messenger._input_channel = connection.channel()
        Messenger._input_channel.queue_declare(queue=INPUT_QUEUE, durable=True)

        # Create output channels
        # Channels identified by their language names
        for lang_name, q_name in OUTPUT_QUEUES.items():
            Messenger._output_channels[lang_name] = connection.channel()
            Messenger._output_channels[lang_name].queue_declare(queue=q_name, durable=True)
            Messenger._output_channels[lang_name].confirm_delivery()

    @staticmethod
    def _consume_input(ch, method, properties, body):
        """Handles input message and calls methods responsible for running
        scanner and sending output messages"""
        ch.stop_consuming()
        body_dec = body.decode('utf-8')
        logging.info("Received a new message: {}".format(body_dec))
        try:
            message = json.loads(body_dec)
            lang_names = Messenger._validate_scan_repo(message)
        except json.decoder.JSONDecodeError as err:
            logging.error("Exception while decoding JSON message: {}".format(err))
        except Exception as e:
            logging.error("Exception while handling input message or scanning repo: {}".format(e))
        else:
            Messenger._send_output(message, lang_names)
            logging.info("Finished extractor task.\n")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def _validate_scan_repo(message):
        """Validates input message and runs RepoScanner on proper repository
        :returns language names found in the repository, returned by
        RepoScanner.run_scanner()"""
        if 'repo_id' not in message:
            raise Exception("Did not found \"repo_id\" entry in the JSON message")
        repo_id = str(message['repo_id'])
        try:
            # Check if repo_id is present in repositories
            if not DbManager.select_repository_by_id(repo_id):
                raise Exception('Did not found repository \'{}\' in the repositories table.'
                                .format(repo_id))
            logging.info("Running repo scanner for \'{}\'".format(repo_id))
            found_languages = RepoScanner.run_scanner(repo_id)
        except Exception as e:
            logging.error("Exception while scanning repository \'{}\'. Aborting further process for this repo.\n"
                          "Cause: {}".format(repo_id, e))
            raise e
        else:
            logging.info("Repository scan complete")
            return found_languages

    @staticmethod
    def _send_output(message, lang_names):
        """Sends output message to all defined _output_channels identified
        by their corresponding language names"""
        for lang_name in lang_names:
            try:
                out_queue_name = OUTPUT_QUEUES[lang_name]
                logging.info("Sending message to {}...".format(out_queue_name))
                Messenger._output_channels[lang_name].basic_publish(
                    exchange='',
                    routing_key=out_queue_name,
                    properties=pika.BasicProperties(delivery_mode=2, ),
                    body=bytes(json.dumps(message), encoding='utf8')
                )
                logging.info("Output message received by RabbitMQ")
            except pika.exceptions.NackError as e:
                logging.error("Output message REJECTED by RabbitMQ (queue full?). Error: {}".format(e))
