import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ↓↓↓ ВСТАВЬТЕ ВАШ API KEY ↓↓↓
API_KEY = ""
# ↑↑↑ ВСТАВЬТЕ ВАШ API KEY ↑↑↑


def force_api_access(spreadsheet_id, range_name="Лист1"):
    """
    Принудительный доступ через Google Sheets API
    """
    try:
        # Создаем сервис
        service = build("sheets", "v4", developerKey=API_KEY)

        print(f" Пробуем подключиться к таблице: {spreadsheet_id}")

        # Пробуем разные методы API
        sheet = service.spreadsheets()

        # Метод 1: Получить значения
        print(" Метод 1: Получение значений...")
        result = (
            sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        )

        values = result.get("values", [])

        if not values:
            print(" Данные не найдены")
            return None

        # Создаем DataFrame
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)

        print(f" УСПЕХ через API! Загружено: {len(data)} строк, {len(headers)} колонок")
        return df

    except HttpError as error:
        error_details = str(error)
        print(f" Ошибка API: {error_details}")

        # Анализируем ошибку
        if "not supported" in error_details:
            print(" ПРОБЛЕМА: Таблица не поддерживает API доступ")
            print("РЕШЕНИЕ: Создайте новую таблицу через sheets.google.com")
        elif "PERMISSION_DENIED" in error_details:
            print(" ПРОБЛЕМА: Нет доступа к таблице")
            print(" РЕШЕНИЕ: Убедитесь, что таблица доступна по ссылке")
        elif "API_KEY_INVALID" in error_details:
            print(" ПРОБЛЕМА: Неверный API Key")
            print(" РЕШЕНИЕ: Проверьте API Key в Google Cloud Console")

        return None


def check_spreadsheet_info(spreadsheet_id):
    """
    Проверка информации о таблице через API
    """
    try:
        service = build("sheets", "v4", developerKey=API_KEY)
        sheet = service.spreadsheets()

        # Получаем метаданные таблицы
        result = sheet.get(spreadsheetId=spreadsheet_id).execute()

        print(" Информация о таблице:")
        print(f"   Название: {result.get('properties', {}).get('title', 'N/A')}")
        print(f"   Количество листов: {len(result.get('sheets', []))}")
        print(
            f"   Время создания: {result.get('properties', {}).get('createdTime', 'N/A')}"
        )

        return True
    except Exception as e:
        print(f" Не удалось получить информацию: {e}")
        return False


def main():
    """
    Основная функция с принудительным API доступом
    """
    print("=== GOOGLE SHEETS API ACCESS ===")
    print(f"API Key: {API_KEY[:15]}...")

    # ↓↓↓ ВСТАВЬТЕ НОВЫЕ ID ТАБЛИЦ ↓↓↓
    NEW_SPREADSHEET_ID_1 = "1Ty6n4XXAGfe_MZVUfRp_R7CmJVUnxlbUAUpCPa63VU0"
    NEW_SPREADSHEET_ID_2 = "1jU1sSmnQScxIFpE22L4vul53xH_ThcZ0h2vnbVs58Xg"

    # Проверяем доступ к таблицам
    print("\n Проверка таблицы 1...")
    check_spreadsheet_info(NEW_SPREADSHEET_ID_1)

    print("\n Проверка таблицы 2...")
    check_spreadsheet_info(NEW_SPREADSHEET_ID_2)

    # Загружаем данные через API
    print("\n Загрузка данных через API...")
    df1 = force_api_access(NEW_SPREADSHEET_ID_1)
    df2 = force_api_access(NEW_SPREADSHEET_ID_2)

    if df1 is not None and df2 is not None:
        print("\n ВСЕ РАБОТАЕТ ЧЕРЕЗ API!")
        print(df1.head(5))
        print(df2.head(5))

        df1.to_csv("api_data1.csv", index=False)
        df2.to_csv("api_data2.csv", index=False)
        print(" Данные сохранены через API!")

    else:
        print("\n НЕ УДАЛОСЬ ИСПОЛЬЗОВАТЬ API")
