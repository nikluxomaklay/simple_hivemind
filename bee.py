import getopt
import os
import random
import string
import sys
import time
from datetime import datetime

from redis import StrictRedis

# Сообщение с подсказкой. Показывается при запуске с параметром 'help'
HELP_TEXT = 'bee.py [getErrors][help]'

# Ключи redis
# Сообщения пишущего процесса
MSGS_KEY = 'bee_msgs'
# PID пишущего процесса
WRITER_KEY = 'writer'
# Время последней записи
WRITING_TIME_KEY = 'writing_time'
# Сообщения, содержащие ошибку
ERR_MSGS_KEY = 'err_msgs'

# Задержки между циклами записи/чтения (сек)
WRITE_DELAY = 0.5
READ_DELAY = 1

PID = os.getpid()


def check_writer(client):
    """
    Проверка существования пишушего процесса.
    Проверяется, записан ли PID пишущего процесса и последнее время записи:
    - Если отсутствует PID, проверяющий процесс станивится пишущим.
    - Если PID записан и равен PID'у проверяющего процесса, значит проверяющий
    процесс уже является пишущим.
    - Если PID записан, но последняя запись была больше 5-и секунд назад, то
    проверяющий процесс становится пишущим.
    """
    writer, last_writing = client.mget(WRITER_KEY, WRITING_TIME_KEY)

    if not writer:
        client.mset({
            WRITER_KEY: PID,
            WRITING_TIME_KEY: datetime.now()
        })
        return True

    if writer and last_writing:
        writer = int(writer)
        if writer == PID:
            return True

        last_writing = datetime.strptime(
            last_writing.decode('utf-8'), '%Y-%m-%d %H:%M:%S.%f'
        )
        if (datetime.now() - last_writing).seconds > 5:
            client.mset({
                WRITER_KEY: PID,
                WRITING_TIME_KEY: datetime.now()
            })
            return True

    return False


def write_msg(client):
    """Запись рандомной строки в redis"""
    client.mset({
        WRITER_KEY: PID,
        WRITING_TIME_KEY: datetime.now()
    })
    msg = ''.join(
        random.choice(
            string.ascii_uppercase + string.digits
        ) for _ in range(5)
    )
    client.rpush(MSGS_KEY, msg)
    time.sleep(WRITE_DELAY)


def read_msg(client):
    """
    Чтение строки из redis.
    С вероятностью 5% прочитанная строка считается содержащей ошибку и
    складывается в redis по ключу ERR_MSGS_KEY.
    """
    msg = client.lpop(MSGS_KEY)

    if msg and random.randint(1, 20) == 1:
        client.rpush(ERR_MSGS_KEY, msg)

    time.sleep(READ_DELAY)


def gather_err_msgs(client):
    """
    Сбор сообщений, содержащих ошибки с их удалением из redis и
    выводом в консоль.
    После этого приложение завершает работу.
    """
    msgs = client.lrange(ERR_MSGS_KEY, 0, -1)
    client.delete(ERR_MSGS_KEY)
    print([msg for msg in msgs])
    sys.exit(0)


def main(argv):
    try:
        opts, args = getopt.getopt(argv, '', ['getErrors', 'help'])
    except getopt.GetoptError:
        print(HELP_TEXT)
        sys.exit(2)

    if 'help' in args:
        print(HELP_TEXT)
        sys.exit(0)

    client = StrictRedis(host='localhost', port=6379)

    if 'getErrors' in args:
        gather_err_msgs(client)

    while True:
        is_writer = check_writer(client)
        if is_writer:
            write_msg(client)
        else:
            read_msg(client)


if __name__ == '__main__':
    main(sys.argv[1:])
