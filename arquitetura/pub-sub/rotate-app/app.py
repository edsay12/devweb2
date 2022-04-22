from PIL import Image, ImageOps
import os
from confluent_kafka import Consumer, KafkaError,Producer
import json
import logging
from time import sleep
import time 
from uuid import uuid4

OUT_FOLDER = '/processed/rotate/'
NEW = '_rotate'
IN_FOLDER = "/appdata/static/uploads/"

def create_rotate(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    original_image = Image.open(path_file)
    transposed = original_image.transpose(Image.Transpose.ROTATE_180)

    name, ext = os.path.splitext(filename)
    transposed.save(output_folder + name + NEW + ext)


#sleep(30)
### Consumer
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'rotate-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['image'])
#{"timestamp": 1649288146.3453217, "new_file": "9PKAyoN.jpeg"}

# criando um novo topico 
TOPIC='notify'
# função com as configs do topico
def publish(topic, filename):
    p = Producer({'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093'})
    p.produce(topic, key=str(uuid4()), value=get_json_str(time.time(), filename,'rotate'), on_delivery=delivery_report)
    p.flush()

def get_json_str(timestamp, filename,mothodType):
    d = {
        'timestamp': timestamp,
        'new_file': filename,
        'MensageType':mothodType
    }
    return json.dumps(d)

# repot delivery
def delivery_report(errmsg, data):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(data.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        data.key(), data.topic(), data.partition(), data.offset()))

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            logging.warning(f"READING {filename}")
            create_rotate(IN_FOLDER + filename)
            logging.warning(f"ENDING {filename}")
            publish(TOPIC,filename) # se nao tiver erros vamos publicar os topicos
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
