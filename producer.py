import pika
import json
import configparser
from faker import Faker
from mongoengine import connect, Document, StringField, BooleanField

config = configparser.ConfigParser()
config.read('config.ini')

mongo_user = config.get('DB', 'user')
mongodb_pass = config.get('DB', 'pass')
db_name = config.get('DB', 'db_name')
domain = config.get('DB', 'domain')

# Підключення до кластеру AtlasDB
connect(host=f"""mongodb+srv://{mongo_user}:{mongodb_pass}@{domain}.utwip5n.mongodb.net/{db_name}?retryWrites=true&w=majority""", ssl=True)

# Модель для контактів
class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    sent_email = BooleanField(default=False)

# Генератор фейкових контактів
def generate_contacts(num_contacts):
    fake = Faker()
    contacts = []
    for _ in range(num_contacts):
        full_name = fake.name()
        email = fake.email()
        contacts.append({"full_name": full_name, "email": email})
    return contacts

# Запис фейкових контактів у базу даних та надсилання ідентифікаторів у чергу RabbitMQ
def send_contacts_to_rabbitmq(contacts):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='email_contacts')

    for contact in contacts:
        new_contact = Contact(full_name=contact["full_name"], email=contact["email"])
        new_contact.save()

        message = {"contact_id": str(new_contact.id)}
        channel.basic_publish(exchange='', routing_key='email_contacts', body=json.dumps(message))

    connection.close()

if __name__ == "__main__":
    num_contacts = 5  # Кількість фейкових контактів для генерації
    contacts = generate_contacts(num_contacts)
    send_contacts_to_rabbitmq(contacts)
    print(f"{num_contacts} фейкових контактів надіслано у чергу RabbitMQ")
