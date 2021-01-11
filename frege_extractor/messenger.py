import json
import logging
import pika
import sys
from db_manager import DbManager
from repo_scanner import RepoScanner


class Messenger:
    """Handles input and output messages in RabbitMQ message-broker.
    Calls other classes' methods for extracting repository."""

    _input_queue = 'extract'

    # Output queues' names identified by language names (from repositories table)
    __output_queue_name = {
        'C': 'analyze-c',
        'C++': 'analyze-cpp',
        'C#': 'analyze-csharp',
        'CSS': 'analyze-css',
        'Java': 'analyze-java',
        'JS': 'analyze-js',
        'PHP': 'analyze-php',
        'Python': 'analyze-python',
        'Ruby': 'analyze-ruby',
    }

    # Output channels identified by their language names
    _output_channels = dict()

    @staticmethod
    def app(rabbitmq_host, rabbitmq_port):
        while True:
            try:
                # Make connection to RabbitMQ
                logging.info(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

                # Create input channel
                input_channel = connection.channel()
                input_channel.queue_declare(queue=Messenger._input_queue, durable=True)

                # Create output channels
                # Channels identified by their language names
                for lang_name, q_name in Messenger.__output_queue_name.items():
                    Messenger._output_channels[lang_name] = connection.channel()
                    Messenger._output_channels[lang_name].queue_declare(queue=q_name, durable=True)
                    Messenger._output_channels[lang_name].confirm_delivery()

                logging.info("Connected.")
                while True:
                    input_channel.basic_consume(
                        queue=Messenger._input_queue,
                        auto_ack=False,
                        on_message_callback=Messenger._consume_input)
                    logging.info(' [*] Waiting for a message about extracting a new repo...')
                    input_channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as exception:
                logging.error("AMQP Connection Error: {}".format(exception))
            except KeyboardInterrupt:
                logging.info(" Exiting...")
                try:
                    connection.close()
                except NameError:
                    pass
                sys.exit(0)

    @staticmethod
    def _consume_input(ch, method, properties, body):
        ch.stop_consuming()
        body_dec = body.decode('utf-8')
        logging.info("Received a new message: {}".format(body_dec))
        try:
            message = json.loads(body_dec)
            lang_names = Messenger._validate_scan_repo(message)
            Messenger._send_output(message, lang_names)
            logging.info("Finished extractor task.\n")
        except json.decoder.JSONDecodeError as err:
            logging.error("Exception while decoding JSON message: {}".format(err))
        except Exception as e:
            logging.error("Exception while handling input message: {}".format(e))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def _validate_scan_repo(message):
        if 'repo_id' not in message:
            raise Exception("Did not found \"repo_id\" entry in the JSON message")
        repo_id = message['repo_id']
        # Run scanner
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
        else:
            logging.info("Repository scan complete")
            return found_languages

    @staticmethod
    def _send_output(message, lang_names):
        for lang_name in lang_names:
            try:
                out_queue_name = Messenger.__output_queue_name[lang_name]
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
