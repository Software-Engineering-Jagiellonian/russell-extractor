import json

import pika
import sys
from extractor.db_manager import DbManager
from extractor.ext_lang_mapper import ExtLangMapper
from extractor.repo_scanner import RepoScanner


class Messenger:
    """Handles input and output messages in RabbitMQ message-broker"""
    # TODO: use database to map output queue names to languages

    _input_queue = 'extract'

    # Output queues' names by language ids
    _output_queues = {
        1: 'analyze-c',
        2: 'analyze-cpp',
        3: 'analyze-csharp',
        4: 'analyze-css',
        5: 'analyze-java',
        6: 'analyze-js',
        7: 'analyze-php',
        8: 'analyze-python',
        9: 'analyze-ruby',
    }

    _output_channels = dict()

    @staticmethod
    def callback(ch, method, properties, body):
        ch.stop_consuming()
        print(f" [x] Received!:")
        print(body.decode('utf-8'))
        input("Press ENTER to start extracting stuff")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def publish_one(rabbitmq_host, rabbitmq_port, queue):
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
        """Publish messages to all queues declared in the list"""
        while True:
            try:
                print(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

                output_channels = dict()
                # Declare output channels
                # Channels identified by their queue names
                for q_name in Messenger._output_queues.values():
                    output_channels[q_name] = connection.channel()
                    output_channels[q_name].queue_declare(queue=q_name, durable=True)
                    output_channels[q_name].confirm_delivery()

                print("Connected")

                while True:
                    print("Type a message and press Ctrl+D:")
                    message = "".join(sys.stdin.readlines())
                    for q_name in Messenger._output_queues.values():
                        try:
                            payload = message + " for " + q_name + " queue"
                            output_channels[q_name].basic_publish(
                                exchange='',
                                routing_key=q_name,
                                properties=pika.BasicProperties(delivery_mode=2,),
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
                print(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

                # Create input channel
                input_channel = connection.channel()
                input_channel.queue_declare(queue=Messenger._input_queue, durable=True)

                # Create output channels
                # Channels identified by their queue names
                for q_name in Messenger._output_queues.values():
                    Messenger._output_channels[q_name] = connection.channel()
                    Messenger._output_channels[q_name].queue_declare(queue=q_name, durable=True)
                    Messenger._output_channels[q_name].confirm_delivery()

                print("Connected")

                while True:
                    input_channel.basic_consume(
                        queue=Messenger._input_queue,
                        auto_ack=False,
                        on_message_callback=Messenger._consume_input)

                    print(' [*] Waiting for a message about extracting a new repo...')
                    input_channel.start_consuming()
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
    def _consume_input(ch, method, properties, body):
        # TODO: handle error for unsuccessful json load
        ch.stop_consuming()
        print(f" Extractor received a new message!")
        try:
            message = json.loads(body.decode('utf-8'))
        except json.decoder.JSONDecodeError as err:
            print("Exception while decoding JSON message:", err)
        else:
            Messenger._scan_send(message)
        input("Press ENTER to start extracting stuff")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def _scan_send(message):
        if 'repo_id' in message:
            repo_id = message['repo_id']
            # Run scanner
            try:
                print("Running repo scanner for \'{}\'".format(repo_id))
                found_languages = RepoScanner.scan_repo(repo_id)
            except Exception as e:
                print("Exception while scanning repository \'{}\'. Aborting further process for this repo."
                      "Cause: {}".format(repo_id, e))
            else:
                # Forward message for all proper output channels
                for lang_id in found_languages:
                    try:
                        Messenger._output_channels[Messenger._output_queues[lang_id]].basic_publish(
                            exchange='',
                            routing_key=Messenger._output_queues[lang_id],
                            properties=pika.BasicProperties(delivery_mode=2,),
                            body=bytes(json.dumps(message), encoding='utf8')
                        )
                        print("Message was received by RabbitMQ")
                    except pika.exceptions.NackError as e:
                        print("Message was REJECTED by RabbitMQ (queue full?). Error:", e)
        else:
            print('Did not found \"repo_id\" in the JSON message.')



if __name__ == '__main__':

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
    Messenger.app('127.0.0.1', '5672')
    # Messenger.publish_all('127.0.0.1', '5672')
    # Messenger.publish_one('127.0.0.1', 5672, 'analyze-c')

