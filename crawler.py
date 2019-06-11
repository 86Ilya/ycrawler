#!/usr/bin/env python3.7

# async modules
import asyncio
import aiohttp
import aiofiles
import async_timeout
from aiohttp.client_exceptions import ClientConnectionError, ServerDisconnectedError
from asyncio import TimeoutError, CancelledError
# other modules
import argparse
import logging
import ssl
import re
import os
import html
import hashlib
import time
import uuid
from collections import defaultdict


LOGGER_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
DATE_FORMAT = '%Y.%m.%d %H:%M:%S'
BASE_URL = "https://news.ycombinator.com/"
COMMENTS_URL = "https://news.ycombinator.com/item?id={}"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULTROOT = os.path.join(BASE_DIR, 'PAGES')
REGEX_TOP_URLS = re.compile(r'id=\'(\d+)\'>[\s\S]*?<a href=\"(.*?)\" class=\"storylink\">[\s\S]*?</a>')
REGEX_SUB_NAME = re.compile(r'[:\/#]')
REGEX_COMMENTSPAN = re.compile(r'<span class=\"commtext c00\">([\s\S]*?)</span>')
REGEX_HREF = re.compile(r'<a href=\"(.*?)\".*?>')
SSLCONTEXT = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
FOLDERNAME_MAXLENGTH = 127
FILENAME_MAXLENGTH = 127
FETCH_TIMEOUT = 15
MAXIMUM_FETCHES = 5


class CrawlerError(Exception):
    pass


class Fetcher:
    """
    Класс обёртка для функции скачивания. Позволяет сохранять ошибки и
    имена скачанных страниц.
    """
    def __init__(self, session):
        self.session = session
        self.errors = dict()
        self.error = "something wrong"
        self.downloaded_pages = list()
        self.black_list = list()
        # Переменная для подсчёта скачанных страниц отдельно для каждой итерации
        self.jobs = defaultdict(int)

    def increment_job_count(self, job_id):
        self.jobs[job_id] += 1

    def get_downloaded_pages_by_job_id(self, job_id):
        return self.jobs.pop(job_id)

    async def fetch(self, url, job_id):
        """
        Простой метод для скачивания ресурсов.
        :param self:
        :param string url: Адрес ресурса который необходимо скачать.
        :return string|bytes: В завимости от того что скачиваем - текст или файл
        """
        for _ in range(MAXIMUM_FETCHES):
            with async_timeout.timeout(FETCH_TIMEOUT):
                try:
                    async with self.session.get(url, ssl=SSLCONTEXT) as response:
                        header = response.headers.get('Content-Type', None)
                        if header and 'text/html' in header:
                            page = await response.text()
                        else:
                            page = await response.read()
                        # Если успешно скачали страницу, то добавим её в список скачанных
                        # Тут будет и заглавная страница и страницы по сслыкам из комментариев
                        self.downloaded_pages.append(url)
                        self.increment_job_count(job_id)
                        return page
                except (ClientConnectionError, ServerDisconnectedError,
                        TimeoutError, UnicodeDecodeError) as error:
                    self.error = str(error)
                except CancelledError as error:
                    self.error = "Cancelled Task"

        # Если мы дошли до этого места, значит имело место быть ошибка.
        # Запишем её. Обработка возвращаемого None производится уже на уровне выше.
        logging.error(self.error)
        self.errors.update({url: self.error})
        # Заодно добавим страницу в "чёрный список", чтобы не зацикливаться на её скачивании
        self.black_list.append(url)


async def get_top_urls(fetcher, base_url, job_id):
    """
    Функция получает на вход базовый url новостного сайта. Парсит его на новостные
    ссылки и возвращает массив со ссылками на новости и id для генерации ссылки комментарии
    к соответствующей новости.
    :param Fetcher fetcher:
    :param string base_url:
    :return list: [(id_comment1, url1), (id_comment2, url2)...]
    """

    response = await fetcher.fetch(base_url, job_id)
    if response is None:
        error = "Ошибка подключения к сайту новостей"
        raise CrawlerError(error)
    search_result = REGEX_TOP_URLS.finditer(response)
    return [(m.group(1), m.group(2)) for m in search_result]


async def download_page_with_comments(fetcher, job_id, root, url, comments_id):
    """
    Функция скачивает страницу и все страницы по ссылкам в комментариях.
    :param Fetcher fetcher:
    :param string root:
    :param string url:
    :param string comments_id:
    :return None:
    """
    tasks = list()
    page = fetcher.fetch(url, job_id)
    tasks.append(save_page(page, root, url, url))
    comments_page = await fetcher.fetch(COMMENTS_URL.format(comments_id), job_id)
    if comments_page is None:
        return
    # TODO 1 regex!!!
    comments = REGEX_COMMENTSPAN.finditer(comments_page)
    for comment in comments:
        for link in REGEX_HREF.finditer(comment.group(1)):
            if link:
                link = html.unescape(link.group(1))
                task = save_page(fetcher.fetch(link, job_id), root, url, link)
                tasks.append(asyncio.create_task(task))

    await asyncio.gather(*tasks)


def create_folder(root, foldername):
    """
    Функция формирует имя папки и создаёт её.
    :param string root: Корневая папка для сохранения скачанных страниц.
    :param string foldername: Желаемое имя создаваемой папки
    :return string page_dir: Имя, которое получилось, с учётом ограничений ОС.
    """
    page_dir = os.path.join(root, foldername)
    # В линуксе есть ограничения на длину имени файла и папки
    if len(page_dir) >= FOLDERNAME_MAXLENGTH:
        hash = hashlib.md5(page_dir.encode()).hexdigest()
        page_dir = page_dir[:FOLDERNAME_MAXLENGTH - len(hash)] + hash

    if not os.path.exists(page_dir):
        os.mkdir(page_dir)

    return page_dir


async def write_file(page_dir, filename, content):
    """
    Функция для непосредственной записи полученной страницы на диск.
    :param string page_dir: имя папки.
    :param string filename: файла для записи.
    :param string content: содержимое для записи.
    :return None:
    """
    if len(filename) >= FILENAME_MAXLENGTH:
        hash = hashlib.md5(filename.encode()).hexdigest()
        filename = filename[:FILENAME_MAXLENGTH - len(hash)] + hash

    page_file = os.path.join(page_dir, filename)
    if not os.path.exists(page_file):
        if isinstance(content, bytes):
            mode = 'wb'
        else:
            mode = 'w'
        async with aiofiles.open(page_file, mode) as out:
            await out.write(content)
            await out.flush()


async def save_page(fetch_courutine, root, foldername, filename):
    """
    Функция сохраняет полученную страницу.
    :param courutine fetch_courutine: Корутина с "содержимым" для записи
    :param string root: Корневая папка для сохранения скачанных страниц.
    :param string foldername: Желаемое имя создаваемой папки
    :param string filename: Желаемое имя для файла
    :return None:
    """
    content = await fetch_courutine

    if content is None:
        return
    foldername = REGEX_SUB_NAME.sub('_', foldername)
    filename = REGEX_SUB_NAME.sub('_', filename)

    page_dir = create_folder(root, foldername)
    await write_file(page_dir, filename, content)


async def main(args):
    """
    :param args:
    :return None:
    """
    async with aiohttp.ClientSession() as session:
        fetcher = Fetcher(session)
        while True:
            try:
                async def one_iteration():
                    job_id = uuid.uuid1()
                    logging.info("Начинаем скачивание страниц")
                    last_fetched_num = len(fetcher.downloaded_pages)
                    top_urls = await get_top_urls(fetcher, BASE_URL, job_id)
                    tasks = list()
                    for comment_id, url in top_urls:
                        if 'item?id=' in url:
                            url = BASE_URL + url
                        url = url.strip()
                        if url not in fetcher.downloaded_pages and url not in fetcher.black_list:
                            tasks.append(download_page_with_comments(fetcher, job_id, args.root, url, comment_id))
                    await asyncio.gather(*tasks)
                    downloaded_pages = fetcher.get_downloaded_pages_by_job_id(job_id)
                    logging.info("Было скачано {} ресурсов".format(downloaded_pages))
                    logging.info("Закончили скачивание страниц")
                # Запустим независимо одну итерацию по скачиванию страниц
                asyncio.create_task(one_iteration())
                await asyncio.sleep(args.period)
            finally:
                for url, error in fetcher.errors.items():
                    print("URL ERROR: {}, MESSAGE: {}".format(url, error))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Краулер для новостного сайта. Скачивает топ-30 страниц и страницы из ссылок в коментариях')
    parser.add_argument('--period', type=int, default=15,
                        help='Период, с которым утилита будет скачивать страницы с сайта.')
    parser.add_argument('--logfile', type=str, default=None,
                        help='Файл для записи логов.')
    parser.add_argument('--root', type=str, default=DEFAULTROOT,
                        help='Файл для записи логов.')
    parser.add_argument('--verbose', action='store_true',
                        help='Detailed output')

    args = parser.parse_args()
    logging.basicConfig(filename=args.logfile, format=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    if args.verbose:
        log.setLevel(logging.DEBUG)
    os.makedirs(args.root, exist_ok=True)
    try:
        asyncio.run(main(args))
    except Exception as error:
        logging.exception(str(error))
