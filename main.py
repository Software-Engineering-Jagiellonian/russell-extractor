import pika
import sys
from extractor.db_manager import DbManager
from extractor.ext_lang_mapper import ExtLangMapper
from extractor.repo_scanner import RepoScanner


class RabbitMQMessenger:
    """Handles input and output messages in RabbitMQ message-broker"""

    @staticmethod
    def callback(ch, method, properties, body):
        ch.stop_consuming()
        print(f" [x] Received!:")
        print(body.decode('utf-8'))
        input("Press ENTER to start extracting stuff")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def app(rabbitmq_host, rabbitmq_port, queue):
        while True:
            try:
                print(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))
                channel = connection.channel()

                input_channel = connection.channel()
                input_channel.queue_declare(queue='extract')

                # Create all output channels for analyzers

                print("Connected")

                channel.queue_declare(queue=queue, durable=True)

                while True:
                    channel.basic_consume(queue=queue,
                                          auto_ack=False,
                                          on_message_callback=RabbitMQMessenger.callback)

                    print(' [*] Waiting for a message about extracting a new repo...')
                    channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as exception:
                print(f"AMQP Connection Error: {exception}")
            except KeyboardInterrupt:
                print(" Exiting...")
                try:
                    connection.close()
                except NameError:
                    pass
                sys.exit(0)



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
    # ExtLangMapper.init_extension_language_ids()
    # RepoScanner.scan_repo('lcs')
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
    app('127.0.0.1', '5672', 'extract')
