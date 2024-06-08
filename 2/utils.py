import json
import aiofiles
import logging


async def read_partnumbers(file_path: str) -> list:
    """
    Чтение партномеров из файла
    :param file_path: путь к файлу
    :return: список партномеров
    """
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
            partnumbers = [line.strip() for line in await file.readlines()]
            logging.info(f"Partnumbers: {partnumbers}")
        return partnumbers
    except Exception as e:
        logging.error(f"IO error: {e}")


async def dump_to_json(partnumber: str, items_list, site: str):
    """
    Запись в json
    :param partnumber:
    :param items_list:
    """
    async with aiofiles.open(f'{partnumber}_{site}.json', 'w', encoding='utf-8') as f:
        await f.write(f"\"nomenclature_name\": [{partnumber}]\n")
        await f.write(json.dumps(items_list, ensure_ascii=False, indent=4))