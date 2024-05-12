import pika
import json
import configparser
from mongoengine import connect
from producer import Contact  # Підключення моделі контактів

config = configparser.ConfigParser()
config.read('config.ini')

mongo_user = config.get('DB', 'user')
mongodb_pass = config.get('DB', 'pass')
db_name = config.get('DB', 'db_name')
domain = config.get('DB', 'domain')

# Підключення до кластеру AtlasDB
connect(host=f"""mongodb+srv://{mongo_user}:{mongodb_pass}@{domain}.utwip5n.mongodb.net/{db_name}?retryWrites=true&w=majority""", ssl=True)

# Функція-заглушка для імітації надсилання електронної пошти
def send_email(email):
    print(f"Надсилаємо електронний лист на адресу: {email}")
    # Тут можна реалізувати логіку реального надсилання пошти

# Функція-консюмер для обробки повідомлень з черги RabbitMQ
def callback(ch, method, properties, body):
    contact_id = json.loads(body)["contact_id"]
    contact = Contact.objects(id=contact_id).first()

    if contact:
        if not contact.sent_email:
            send_email(contact.email)
            contact.sent_email = True
            contact.save()
            print(f"Повідомлення надіслано для контакту з ID {contact_id}")
        else:
            print(f"Повідомлення вже надіслано для контакту з ID {contact_id}")
    else:
        print(f"Контакт з ID {contact_id} не знайдено")

if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='email_contacts')

    channel.basic_consume(queue='email_contacts', on_message_callback=callback, auto_ack=True)

    print(' [*] Очікування повідомлень. Для виходу натисніть CTRL+C')
    channel.start_consuming()
