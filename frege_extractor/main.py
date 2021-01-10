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
    def publish_one(rabbitmq_host, rabbitmq_port, queue):
        """TEST METHOD. Publish message to specified queue"""
        while True:
            try:
                print(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))
                channel = connection.channel()
                print("Connected")

                channel.confirm_delivery()

                channel.queue_declare(queue=queue, durable=True)

                while True:
                    print("Paste JSON message and press Ctrl+D (Ctrl+Z on Windows) to send it:")
                    payload = "".join(sys.stdin.readlines())
                    try:
                        channel.basic_publish(exchange='',
                                              routing_key=queue,
                                              properties=pika.BasicProperties(
                                                  delivery_mode=2,  # make message persistent
                                              ),
                                              body=bytes(payload, encoding='utf8'))
                        print("Message was received by RabbitMQ")
                    except pika.exceptions.NackError:
                        print("Message was REJECTED by RabbitMQ (queue full?) !")

            except pika.exceptions.AMQPConnectionError as exception:
                print(f"AMQP Connection Error: {exception}")
            except KeyboardInterrupt:
                print(" Exiting...")
                try:
                    connection.close()
                except NameError:
                    pass
                sys.exit(0)

    @staticmethod
    def publish_all(rabbitmq_host, rabbitmq_port):
        """TEST_METHOD. Publish messages to all queues declared in the list"""
        while True:
            try:
                print(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

                output_channels = dict()
                # Declare output channels
                # Channels identified by their queue names
                for q_name in Messenger.__output_queue_name.values():
                    output_channels[q_name] = connection.channel()
                    output_channels[q_name].queue_declare(queue=q_name, durable=True)
                    output_channels[q_name].confirm_delivery()

                print("Connected")

                while True:
                    print("Type a message and press Ctrl+D:")
                    message = "".join(sys.stdin.readlines())
                    for q_name in Messenger.__output_queue_name.values():
                        try:
                            payload = message + " for " + q_name + " queue"
                            output_channels[q_name].basic_publish(
                                exchange='',
                                routing_key=q_name,
                                properties=pika.BasicProperties(delivery_mode=2, ),
                                body=bytes(payload, encoding='utf8')
                            )
                            print("Message was received by RabbitMQ")
                        except pika.exceptions.NackError as e:
                            print("Message was REJECTED by RabbitMQ (queue full?). Error:", e)

            except pika.exceptions.AMQPConnectionError as exception:
                print(f"AMQP Connection Error: {exception}")
            except KeyboardInterrupt:
                print(" Exiting...")
                try:
                    connection.close()
                except NameError:
                    pass
                sys.exit(0)

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
                logging.error(f"AMQP Connection Error: {exception}")
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
        except json.decoder.JSONDecodeError as err:
            logging.error("Exception while decoding JSON message:", err)
        else:
            Messenger._scan_send(message)
            logging.info("Finished extractor task.\n")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def _scan_send(message):
        if 'repo_id' in message:
            repo_id = message['repo_id']
            # Run scanner
            try:
                # Check if repo_id is present in repositories
                if not DbManager.select_repository_by_id(repo_id):
                    raise Exception('Did not found repository \'{}\' in the repositories table.'
                                    .format(repo_id))
                logging.info("Running repo scanner for \'{}\'".format(repo_id))
                found_languages = RepoScanner.scan_repo(repo_id)
            except Exception as e:
                logging.error("Exception while scanning repository \'{}\'. Aborting further process for this repo.\n"
                              "Cause of the error: {}".format(repo_id, e))
            else:
                logging.info("Repository scan complete")
                # Send message for all proper output channels
                for lang_name in found_languages:
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
                        logging.error("Output message REJECTED by RabbitMQ (queue full?). Error:", e)
        else:
            logging.error("Error: did not found \"repo_id\" entry in the JSON message.")


if __name__ == '__main__':
    print("running main.py")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s: %(message)s',
                        datefmt='%H:%M:%S')
    # print(DbManager.insert_repository_language('lcs', 7, True))
    # print(DbManager.insert_repository_languages('fibonacci'))
    # DbManager.update_repository_language_present('fibonacci', 2)
    # rs = DbManager.insert_repository_languages('fibonacci')
    # print(rs)
    # ExtLangMapper.init_extension_language_ids()
    # print(ExtLangMapper.extension_lang_id)
    # os.listdir('..')
    # print(os.path.abspath('.'))
    # ExtLangMapper.init()
    # RepoScanner.scan_repo('fibonacci')
    # DbManager.insert_repository_language_file(87, 'src\\fake-file.cpp')
    # print(DbManager.select_languages())
    # print(DbManager.select_languages())
    # print(DbManager.select_repository_languages('lcs'))

    # try:
    #     rabbitmq_host = os.environ['RABBITMQ_HOST']
    # except KeyError:
    #     print("RabbitMQ host must be provided as RABBITMQ_HOST environment var!")
    #     sys.exit(1)
    #
    # try:
    #     rabbitmq_port = int(os.environ.get('RABBITMQ_PORT', '5672'))
    # except ValueError:
    #     print("RABBITMQ_PORT must be an integer")
    #     sys.exit(2)
    #
    # try:
    #     queue = os.environ['QUEUE']
    # except KeyError:
    #     print("Source queue must be provided as QUEUE environment var!")
    #     sys.exit(3)
    #

    # Connect to RabbitMQ at socket: 127.0.0.1:5672
    # ExtLangMapper.init()

    # rs1 = DbManager.select_repository_by_id('fibonacci')
    # rs2 = DbManager.select_repository_by_id('fibonaccis')
    # print(len(rs1))
    # print(len(rs2))
    Messenger.app('127.0.0.1', '5672')

    # Messenger.publish_all('127.0.0.1', '5672')
    # Messenger.publish_one('127.0.0.1', 5672, 'analyze-c')
