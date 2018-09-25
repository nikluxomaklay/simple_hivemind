import argparse
import os
import random
import string
import sys
import time

from redis import StrictRedis


# Ключи redis
# Сообщения пишущего процесса
MSGS_KEY = 'bee_msgs'
# ID пишущего процесса
WRITER_KEY = 'writer'
# Сообщения, содержащие ошибку
ERR_MSGS_KEY = 'err_msgs'

# Задержки между циклами записи/чтения (сек)
WRITE_DELAY = 0.5
READ_DELAY = 1

# Время хранения ID пишущего процесса (ms)
WRITER_KEY_EXPIRE = 5000

# ID текущего процесса. Состоит из PID и 5-и рандомных ascii_uppercase
ID = f'{os.getpid()}_{"".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))}'  # noqa


def parse_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-e', '--errors',
        help='Собрать сообщения с ошибками',
        action='store_true'
    )

    return arg_parser.parse_args()


def check_writer(client):
    """
    Проверка существования пишушего процесса.
    Проверяется, записан ли ID пишущего процесса:
    - Если отсутствует ID, проверяющий процесс станивится пишущим.
    - Если ID записан и равен ID проверяющего процесса, значит проверяющий
    процесс уже является пишущим.

    Для WRITER_KEY задаётся expire в WRITER_KEY_EXPIRE ms.
    """
    writer = client.get(WRITER_KEY)

    if not writer or writer.decode('utf-8') == ID:
        # Даже если процесс уже является пишущим, нужно обновить значение.
        client.psetex(WRITER_KEY, WRITER_KEY_EXPIRE, ID)
        return True

    return False


def write_msg(client):
    """Запись рандомной строки в redis"""
    client.psetex(WRITER_KEY, WRITER_KEY_EXPIRE, ID)
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


def main_loop(client):
    while True:
        is_writer = check_writer(client)
        if is_writer:
            write_msg(client)
        else:
            read_msg(client)


def gather_err_msgs(client):
    """
    Сбор сообщений, содержащих ошибки с их удалением из redis и
    выводом в консоль.
    После этого приложение завершает работу.
    """
    msgs = client.lrange(ERR_MSGS_KEY, 0, -1)
    client.delete(ERR_MSGS_KEY)
    print([msg.decode('utf-8') for msg in msgs])


def main(get_errors=False):
    client = StrictRedis(host='localhost', port=6379)

    if get_errors:
        gather_err_msgs(client)
        sys.exit(0)

    main_loop(client)


if __name__ == '__main__':
    args = parse_args()
    main(get_errors=args.errors)
