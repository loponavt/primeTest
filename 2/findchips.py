import logging
import httpx
import time
import asyncio
import utils
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)

URL = 'https://www.findchips.com/search/'


async def fetch(session, url):
    """ Асинхронное подключение """
    try:
        response = await session.get(url)
        response.raise_for_status()
        return response.text
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error: {e}")
    except httpx.ConnectTimeout:
        logging.error(f"Timeout error for {url}")


async def parsing(session, partnumber: str):
    """
    Парсит страницу по партномеру
    :param session: HTTP сессия
    :param partnumber: партномер
    """
    request = f'{URL}{partnumber}?currency=USD'
    page = await fetch(session, request)
    if not page:
        print('Ошибка парсинга позиции')
        return
    soup = BeautifulSoup(page, features="lxml")
    items = []
    distributors = soup.find_all(class_='distributor-results')
    for distributor in distributors:

        if not distributor.find('td', class_='td-price').getText().split():
            continue

        subpositions = distributor.find_all('tr', class_='row')

        for subposition in subpositions:
            prices = subposition.find('td', class_='td-price').getText().split()
            if not prices or len(prices) % 2 != 0:
                continue
            item_dict = {"name": "",
                         "vendor": "",
                         "seller": "",
                         "MoQ": "",
                         "PQ": "",
                         "prices": "",
                         "in_stock": "",
                         "lead_time_days": "",
                         "date_code": "",
                         "short_description": "",
                         "seller_url": ""}

            if subposition.find('td', class_='td-part first').find('a', class_='onclick'):
                item_dict['name'] = ' '.join(subposition.find('td', class_='td-part first').find('a', class_='onclick')
                                             .getText().split())

            if subposition.find('td', class_='td-mfg'):
                item_dict['vendor'] = ' '.join(subposition.find('td', class_='td-mfg').getText().split())

            if distributor.find(class_='distributor-title'):
                item_dict['seller'] = ' '.join(distributor.find(class_='distributor-title').getText().split())

            if subposition.find('span', class_='additional-value', attrs={'data-title': 'Min Qty'}):
                item_dict['MoQ'] = subposition.find('span', class_='additional-value',
                                                    attrs={'data-title': 'Min Qty'}).getText()

            if subposition.find('span', class_='additional-value', attrs={'data-title': 'Package Mult.'}):
                item_dict['PQ'] = subposition.find('span', class_='additional-value',
                                                   attrs={'data-title': 'Package Mult.'}).getText()

            if 'See' in prices:
                prices.remove('See')
            if 'More' in prices:
                prices.remove('More')

            item_dict['prices'] = '[' + '; '.join(
                f"'{prices[i]}', '{prices[i + 1]}'" for i in range(0, len(prices), 2)) + ']'

            if subposition.find('td', class_='td-stock'):
                item_dict['in_stock'] = ' '.join(subposition.find('td', class_='td-stock').getText().split())

            if subposition.find('span', class_='additional-value', attrs={'data-title': 'Lead time'}):
                item_dict['lead_time_days'] = subposition.find('span', class_='additional-value',
                                                               attrs={'data-title': 'Lead time'}).getText()

            if subposition.find('span', class_='additional-value', attrs={'data-title': 'Date Code'}):
                item_dict['date_code'] = subposition.find('span', class_='additional-value',
                                                          attrs={'data-title': 'Date Code'}).getText()

            if subposition.find(class_='td-description more'):
                item_dict['short_description'] = subposition.find(class_='td-description more').getText()

            if subposition.find('a', class_='onclick')['href']:
                item_dict['seller_url'] = f"https:{subposition.find('a', class_='onclick')['href']}"

            items.append(item_dict)

    if items:
        await utils.dump_to_json(partnumber, items, 'findchips')


async def main():
    partnumbers = await utils.read_partnumbers('partnumbers.txt')
    if not partnumbers:
        return
    timeout = httpx.Timeout(100.0, read=60.0, write=10.0, connect=15.0)
    # limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    async with httpx.AsyncClient(timeout=timeout) as session:
        tasks = [parsing(session, partnumber) for partnumber in partnumbers]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    tic = time.perf_counter()
    asyncio.run(main())
    tac = time.perf_counter()
    logging.info(f"{tac - tic:0.4f} seconds")
