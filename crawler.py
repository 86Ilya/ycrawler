#!/usr/bin/env python3.7

# async modules
import asyncio
import aiohttp
import aiofiles
import async_timeout
from aiohttp.client_exceptions import ClientConnectionError, ServerDisconnectedError
from asyncio import TimeoutError
# other modules
import argparse
import logging
import ssl
import re
import os
import html
import hashlib
import time

LOGGER_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
DATE_FORMAT = '%Y.%m.%d %H:%M:%S'
BASE_URL = "https://news.ycombinator.com/"
COMMENTS_URL = "https://news.ycombinator.com/item?id={}"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULTROOT = os.path.join(BASE_DIR, 'PAGES')
REGEX_TOP_URLS = re.compile(r'id=\'(\d+)\'>[\s\S]*?<a href=\"(.*?)\" class=\"storylink\">[\s\S]*?</a>')
REGEX_SUB_FOLDERNAME = re.compile(r'[:\/#]')
REGEX_COMMENTSPAN = re.compile(r'<span class=\"commtext c00\">([\s\S]*?)</span>')
REGEX_HREF = re.compile(r'<a href=\"(.*?)\".*?>')
SSLCONTEXT = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
FOLDERNAME_MAXLENGTH = 127
FILENAME_MAXLENGTH = 127
FETCH_TIMEOUT = 15
MAXIMUM_FETCHES = 5


class CrawlerError(Exception):
    pass


class Getter:
    def __init__(self, session):
        self.session = session
        self.errors = dict()
        self.error = "something wrong"
        self.downloaded_pages = list()

    async def fetch(self, url):
        import concurrent
        for _ in range(MAXIMUM_FETCHES):
            with async_timeout.timeout(FETCH_TIMEOUT):
                try:
                    async with self.session.get(url, ssl=SSLCONTEXT) as response:
                        if 'text/html' in response.headers['Content-Type']:
                            page = await response.text()
                            # Если успешно скачали страницу, то добавим её в список скачанных
                            # Тут будет и заглавная страница и страницы по сслыкам из комментариев
                            self.downloaded_pages.append(url)
                            return page
                        else:
                            # same
                            self.downloaded_pages.append(url)
                            page = await response.read()
                            return page
                except (ClientConnectionError, ServerDisconnectedError, TimeoutError) as error:
                    error = str(error)
                    logging.error(error)
                    self.error = error

        #  Если мы дошли до этого места, значит имело место быть ошибка.
        #  Запишем её. Обработка возвращаемого None производится уже на уровне выше.
        self.errors.update({url: self.error})


async def get_top_urls(getter, base_url):
        response = await getter.fetch(base_url)
        if response is None:
            error = "Ошибка подключения к сайту новостей"
            raise CrawlerError(error)
        search_result = REGEX_TOP_URLS.finditer(response)
        return [(m.group(1), m.group(2)) for m in search_result]


async def download_page_with_comments(getter, root, url, comments_id):
    tasks = list()
    page = getter.fetch(url)
    tasks.append(save_page(page, root, url, url))
    comments_page = await getter.fetch(COMMENTS_URL.format(comments_id))
    if comments_page is None:
        return
    # TODO 1 regex!!!
    comments = REGEX_COMMENTSPAN.finditer(comments_page)
    if comments:
        for comment in comments:
            for link in REGEX_HREF.finditer(comment.group(1)):
                if link:
                    link = html.unescape(link.group(1))
                    task = save_page(getter.fetch(link), root, url, link)
                    tasks.append(task)
    await asyncio.ensure_future(asyncio.gather(*tasks))


def create_folder(root, foldername):
    page_dir = os.path.join(root, foldername)
    if len(page_dir) >= FOLDERNAME_MAXLENGTH:
        hash = hashlib.md5(page_dir.encode()).hexdigest()
        page_dir = page_dir[:FOLDERNAME_MAXLENGTH - len(hash)] + hash

    if not os.path.exists(page_dir):
        os.mkdir(page_dir)

    return page_dir


async def write_file(page_dir, filename, content):
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
    content = await fetch_courutine

    if content is None:
        return
    foldername = REGEX_SUB_FOLDERNAME.sub('_', foldername)
    filename = REGEX_SUB_FOLDERNAME.sub('_', filename)

    page_dir = create_folder(root, foldername)
    await write_file(page_dir, filename, content)


async def main(args):
    async with aiohttp.ClientSession(loop=loop) as session:
        getter = Getter(session)
        while True:
            try:
                last_fetched_num = len(getter.downloaded_pages)
                top_urls = await get_top_urls(getter, BASE_URL)
                tasks = list()
                for comment_id, url in top_urls:
                    if 'item?id=' in url:
                        url = BASE_URL+url
                    url = url.strip()
                    if url not in getter.downloaded_pages:
                        tasks.append(download_page_with_comments(getter, args.root, url, comment_id))
                await asyncio.gather(*tasks)
                cur_fetched_num = len(getter.downloaded_pages)

                logging.info("Было скачано {} ресурсов".format(cur_fetched_num - last_fetched_num))
                logging.info("ERRORS:", getter.errors)
            except KeyboardInterrupt:
                break
            time.sleep(args.period)


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
    loop = asyncio.get_event_loop()
    try:
        logging.info("Начинаем скачивание страниц")
        loop.run_until_complete(main(args))
        logging.info("Закончили скачивание страниц")
    except Exception as error:
        logging.exception(str(error))
    loop.close()
