
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Установка зависимостей
#!pip install python-docx docxcompose beautifulsoup4 ebooklib aiogram aiofiles nest_asyncio

import os
import re
import time
import docx
from docx import Document
from docxcompose.composer import Composer
from bs4 import BeautifulSoup
import ebooklib
from ebooklib import epub
from aiogram import Bot, Router, types, F, Dispatcher
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.utils import markdown as md
import aiofiles
import asyncio
import nest_asyncio
import concurrent.futures
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.fsm.state import StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from functools import partial
nest_asyncio.apply()

# Создаем пул потоков для выполнения CPU-bound задач
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Декоратор для измерения времени выполнения функции
def timer(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        elapsed = time.time() - start_time
        print(f"[PROFILING] Функция {func.__name__} выполнилась за {elapsed:.2f} секунд")
        return result
    return wrapper

# Замените токен на свой
API_TOKEN = '7692853253:AAHGfbJhall58TafIqTBdAujVnuXhhHCwYk'

bot = Bot(token=API_TOKEN)
router = Router()

# ===================== Неблокирующие функции конвертации =====================

# Функция-обертка для выполнения блокирующих операций в отдельном потоке
async def run_in_threadpool(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    func_partial = partial(func, *args, **kwargs)
    return await loop.run_in_executor(thread_pool, func_partial)

# Неблокирующие версии функций конвертации
async def convert_epub_to_docx(epub_file, docx_file):
    def _convert():
        # Открываем EPUB-файл
        book = epub.read_epub(epub_file)
        document = Document()
        # Перебираем элементы книги
        for item in book.get_items():
            if item.get_type() == ebooklib.ITEM_DOCUMENT:
                soup = BeautifulSoup(item.content, 'html.parser')
                for element in soup.find_all():
                    if element.name == 'h1':
                        document.add_heading(element.get_text(), level=0)
                    elif element.name == 'p':
                        doc_paragraph = document.add_paragraph()
                        # Перебор вложенных элементов абзаца
                        for sub in element.contents:
                            if hasattr(sub, 'name'):
                                if sub.name == 'strong':
                                    run = doc_paragraph.add_run(sub.get_text())
                                    run.bold = True
                                elif sub.name == 'em':
                                    run = doc_paragraph.add_run(sub.get_text())
                                    run.italic = True
                                else:
                                    doc_paragraph.add_run(sub.get_text())
                            else:
                                # Если это просто текст
                                doc_paragraph.add_run(sub)
        document.save(docx_file)

    return await run_in_threadpool(_convert)

async def convert_fb2_to_docx(fb2_file, docx_file):
    def _convert():
        with open(fb2_file, 'r', encoding='utf-8') as f:
            content = f.read()
        soup = BeautifulSoup(content, 'xml')
        document = Document()
        for element in soup.find_all():
            if element.name == 'title':
                document.add_heading(element.get_text(), level=0)
            elif element.name == 'p':
                # Если абзац не является частью title или annotation
                if element.find_parent(['title', 'annotation']) is None:
                    doc_paragraph = document.add_paragraph()
                    for sub in element.contents:
                        if hasattr(sub, 'name'):
                            if sub.name == 'strong':
                                run = doc_paragraph.add_run(sub.get_text())
                                run.bold = True
                            elif sub.name == 'emphasis':
                                run = doc_paragraph.add_run(sub.get_text())
                                run.italic = True
                            else:
                                    doc_paragraph.add_run(sub.get_text())
                        else:
                            doc_paragraph.add_run(sub)
        document.save(docx_file)

    return await run_in_threadpool(_convert)

async def convert_txt_to_docx(txt_file, docx_file):
    def _convert():
        with open(txt_file, 'r', encoding='utf-8') as f:
            text = f.read()
        document = Document()
        for line in text.splitlines():
            document.add_paragraph(line)
        document.save(docx_file)

    return await run_in_threadpool(_convert)

@timer
async def process_files(file_list):
    """
    Обрабатывает список файлов, конвертируя их в формат .docx (если требуется)
    и возвращает список имен файлов в формате .docx для последующего объединения.
    """
    converted_files = []
    for file in file_list:
        ext = os.path.splitext(file)[1].lower()
        # Если файл уже в формате .docx – добавляем его в список
        if ext == ".docx":
            converted_files.append(file)
        elif ext == ".txt":
            docx_file = os.path.splitext(file)[0] + ".docx"
            await convert_txt_to_docx(file, docx_file)
            converted_files.append(docx_file)
        elif ext == ".fb2":
            docx_file = os.path.splitext(file)[0] + ".docx"
            await convert_fb2_to_docx(file, docx_file)
            converted_files.append(docx_file)
        elif ext == ".epub":
            docx_file = os.path.splitext(file)[0] + ".docx"
            await convert_epub_to_docx(file, docx_file)
            converted_files.append(docx_file)
    return converted_files

# ===================== Неблокирующие функции для работы с документами =====================
async def check_and_add_title(doc, file_name):
    def _process():
        """
        Проверяет первые абзацы документа на наличие заголовка (например, "Глава ...").
        Если заголовок не найден, добавляет его на основе имени файла.
        """
        patterns = [
            r'Глава[ ]{0,4}\d{1,4}',
            r'Часть[ ]{0,4}\d{1,4}',
            r'^Пролог[ .!]*$',
            r'^Описание[ .!]*$',
            r'^Аннотация[ .!]*$',
            r'^Annotation[ .!]*$',
            r'^Предисловие от автора[ .!]*$'
        ]
        if doc.paragraphs:
            check_paragraphs = doc.paragraphs[0:4]
            title_found = False
            for p in check_paragraphs:
                for pattern in patterns:
                    if re.search(pattern, p.text):
                        title_found = True
                        break
                if title_found:
                    break
            if not title_found:
                # Добавляем заголовок перед первым абзацем
                title = os.path.splitext(os.path.basename(file_name))[0]
                title_run = doc.paragraphs[0].insert_paragraph_before().add_run(f"{title}\n")
                # Форматирование заголовка
                title_run.bold = True
        return doc

    return await run_in_threadpool(_process)

@timer
async def merge_docx(file_list, output_file_name):
    def _merge(files_to_merge):
        # Создаем новый документ
        merged_document = Document(files_to_merge[0])
        composer = Composer(merged_document)

        for file in files_to_merge[1:]:
            doc = Document(file)
            composer.append(doc)

        # Сохраняем итоговый документ
        composer.save(output_file_name)
        print(f"Файлы объединены в {output_file_name}")
        return output_file_name

    # Сначала обрабатываем заголовки для всех файлов
    processed_files = []
    for file in file_list:
        doc = Document(file)
        processed_doc = await check_and_add_title(doc, file)

        # Сохраняем промежуточный файл с добавленным заголовком
        temp_file = f"temp_{os.path.basename(file)}"
        processed_doc.save(temp_file)
        processed_files.append(temp_file)

    # Объединяем обработанные файлы в отдельном потоке
    result = await run_in_threadpool(_merge, processed_files)

    # Удаляем временные файлы
    for file in processed_files:
        if os.path.exists(file):
            os.remove(file)

    return result

# ===================== FSM: Состояния =====================
class MergeStates(StatesGroup):
    collecting = State()  # Состояние сбора файлов

# ===================== Обработчики Telegram-бота =====================
@router.message(Command("start_merge"))
async def start_merge(message: Message, state: FSMContext):
    """
    Команда для начала сбора файлов.
    """
    current_state = await state.get_state()
    if current_state == MergeStates.collecting.state:
        await message.answer("Сбор файлов уже запущен.")
        return

    await state.set_state(MergeStates.collecting)
    await state.update_data(file_list=[])  # Создаем пустой список файлов
    await message.answer("Сбор файлов начат! Отправляйте файлы. Используйте /end_merge для завершения.")

@router.message(Command("end_merge"))
async def end_merge(message: Message, state: FSMContext):
    """
    Команда для завершения сбора файлов и запуска объединения.
    """
    current_state = await state.get_state()
    if current_state != MergeStates.collecting.state:
        await message.answer("Сбор файлов не был запущен. Введите /start_merge для начала.")
        return

    user_data = await state.get_data()
    file_list = user_data.get('file_list', [])

    if not file_list:
        await message.answer("Нет файлов для обработки!")
        await state.clear()  # Очищаем состояние
        return

    # Сообщаем пользователю, что начинаем обработку
    await message.answer(f"Начинаю обработку {len(file_list)} файлов. Это может занять некоторое время...")

    # Создаем уникальное имя выходного файла для этого пользователя
    user_output_file = f"merged_{message.from_user.id}_{int(time.time())}.docx"

    # Асинхронно обрабатываем файлы в фоновом режиме
    asyncio.create_task(process_and_merge_files(message, file_list, user_output_file, state))

    # Сброс состояния после запуска задачи
    await state.clear()

async def process_and_merge_files(message, file_list, output_file_name, state):
    """
    Асинхронная функция для обработки и объединения файлов в фоновом режиме.
    """
    try:
        # Обработка и конвертация файлов
        converted_files = await process_files(file_list)
        merged_file = await merge_docx(converted_files, output_file_name)

        # Формируем сообщение с информацией о собранных файлах
        file_list_str = "\n".join([os.path.basename(f) for f in file_list])
        await message.answer(f"Файлы объединены в {os.path.basename(output_file_name)}.\nСобрано {len(file_list)} файлов:\n{file_list_str}")

        # Отправляем объединённый файл обратно пользователю
        document = FSInputFile(merged_file)
        await message.answer_document(document, caption="Ваш объединённый документ")

        # Удаляем временные файлы
        for file in file_list:
            if os.path.exists(file):
                os.remove(file)

        # Удаляем объединенный файл после отправки
        if os.path.exists(merged_file):
            os.remove(merged_file)

    except Exception as e:
        await message.answer(f"Произошла ошибка при обработке файлов: {str(e)}")

@router.message(F.document)
async def handle_document(message: Message, state: FSMContext):
    """
    Обработчик полученных файлов.
    Если сбор файлов запущен, сохраняет полученный документ на диск
    и добавляет его имя в список для дальнейшей обработки.
    """
    current_state = await state.get_state()
    if current_state != MergeStates.collecting.state:
        await message.answer("Сбор файлов не запущен. Введите /start_merge для начала.")
        return

    try:
        file_info = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        file_name = message.document.file_name

        # Добавляем user_id к имени файла, чтобы избежать конфликтов между пользователями
        base_name, extension = os.path.splitext(file_name)
        file_name = f"{base_name}_{message.from_user.id}{extension}"
        counter = 1

        if extension.lower() not in (".docx", ".fb2", ".txt", ".epub"):
            await message.answer(f"Неизвестный формат файла: {message.document.file_name}. Пожалуйста, отправляйте файлы только в форматах docx, fb2, epub, txt.")
            return

        while os.path.exists(file_name):
            file_name = f"{base_name}_{message.from_user.id}({counter}){extension}"
            counter += 1

        # Сохраняем файл на диск
        async with aiofiles.open(file_name, 'wb') as new_file:
            await new_file.write(downloaded_file.read())

        # Добавляем файл в список
        user_data = await state.get_data()
        file_list = user_data.get('file_list', [])
        file_list.append(file_name)
        await state.update_data(file_list=file_list)

        await message.answer(f"Файл {message.document.file_name} сохранён! Всего файлов: {len(file_list)}")
    except Exception as e:
        await message.answer(f"Ошибка при сохранении файла: {str(e)}")

@router.message(Command("start"))
async def send_welcome(message: Message):
    await message.answer("Привет, я бот для объединения файлов! Нажми /info для получения дополнительной информации.")

@router.message(Command("info"))
async def send_info(message: Message):
    await message.answer("Данный бот объединяет файлы в формате docx. Если формат файла не docx, то файл конвертируется в этот формат и объединяется. Для конвертации поддерживаются форматы fb2, epub, txt. В процессе конвертации сохраняется лишь текст, жирный и курсивный формат и оглавление, всё остальное теряется. Для начала работы нажми на /start_merge. После отправь файлы и нажми на /end_merge, чтобы завершить работу. Удачи!")

# ===================== Запуск бота =====================
async def main():
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    print("Бот запущен.")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
