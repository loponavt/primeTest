import logging
import httpx
import time
import asyncio
import utils

logging.basicConfig(level=logging.INFO)

URL = 'https://en.sekorm.com/pageSearch?&tab=4&searchWord='


async def fetch(session, url) -> dict:
    """ Асинхронное подключение """
    try:
        response = await session.get(url)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error: {e}")
    except httpx.ConnectTimeout:
        logging.error(f"Timeout error for {url}")


def parse_page(items_list: list, page: dict, items_count: int):
    """
    Парсит страницу с позициями
    :param items_list: итоговый list с позициями
    :param page: страница с позициями
    :param items_count: количество позиций на странице
    """
    for item_number in range(items_count):
        try:
            item = page['data']['page']['results'][item_number]
            price = item['fmtUnitPrice']
            if price is None:
                continue
            item_dict = {'name': item['pnCode'],
                         'vendor': item['relateBrandName'],
                         'seller': item['shopName'],
                         'MoQ': item['fmtMinPackAmount'],
                         'PQ': '',  # не нашел такого
                         'prices': price,
                         'in_stock': item['displayStock'],
                         'lead_time_days': item['expectedDelivery'],
                         'date_code': '',  # не нашел такого
                         'short_description': item['description'],
                         'seller_url': f"https://en.sekorm.com/product/{item['id']}.html"}
            items_list.append(item_dict)
        except KeyError as ke:
            logging.error(f'Ошибка при парсинге. Нет такого ключа: {ke}')
        except IndexError as ie:
            logging.error(f'Ошибка при парсинге. Индекс вне диапазона: {ie}')


async def parsing(session, partnumber: str):
    """
    Парсит первую страницу по партномеру и если есть еще страницы запускает их парсинг
    :param session: HTTP сессия
    :param partnumber: партномер
    """
    first_page_url = f'{URL}{partnumber}&currentPage=1'

    items_list = []
    first_page = await fetch(session, first_page_url)
    if not first_page:
        return
    try:
        total_count = first_page['data']['page']['totalCount']
    except KeyError as ke:
        logging.error(f'Ошибка при парсинге. Нет такого ключа: {ke}')
        return
    integer_pages = total_count // 50  # целое число страниц кратное 50
    remainder = total_count % 50  # остаток позиций на последней странице если не кратно 50

    pages_to_parse = []
    for page_number in range(1, integer_pages + 2):
        url = f'{URL}{partnumber}&currentPage={page_number}'
        pages_to_parse.append(fetch(session, url))
    pages = await asyncio.gather(*pages_to_parse)

    for page in pages[:-1]:
        if page:
            parse_page(items_list, page, 50)
    if remainder != 0 and pages[-1]:
        parse_page(items_list, pages[-1], remainder)

    if items_list:
        await utils.dump_to_json(partnumber, items_list, 'sekorm')


async def main():
    partnumbers = await utils.read_partnumbers('partnumbers.txt')
    if not partnumbers:
        return

    timeout = httpx.Timeout(60.0, read=10.0, write=30.0, connect=15.0)
    # limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    async with httpx.AsyncClient(timeout=timeout) as session:
        tasks = [parsing(session, partnumber) for partnumber in partnumbers]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    tic = time.perf_counter()
    asyncio.run(main())
    tac = time.perf_counter()
    logging.info(f"{tac - tic:0.4f} seconds")
