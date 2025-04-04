#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Установка зависимостей
!pip install python-docx docxcompose beautifulsoup4 ebooklib aiogram aiofiles nest_asyncio

import os
import re
import time
import docx
from docx import Document
from docxcompose.composer import Composer
from bs4 import BeautifulSoup
import ebooklib
from ebooklib import epub
from aiogram import Bot, Router, types, F, Dispatcher
from aiogram.types import Message, FSInputFile
from aiogram.filters import Command
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.utils import markdown as md
import aiofiles
import asyncio
import nest_asyncio
import concurrent.futures
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.fsm.state import StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from functools import partial
from collections import deque
from datetime import datetime, timezone, timedelta
nest_asyncio.apply()

# Создаем пул потоков для выполнения CPU-bound задач
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

class UserLimits:
    def __init__(self):
        self.user_data = {}  # {user_id: {'files_today': int}}
        self.last_global_reset = self._get_last_utc_midnight()
        self.user_locks = {} # Словарь для хранения блокировок пользователей

    def _get_last_utc_midnight(self):
        """Возвращает последнюю полночь по UTC."""
        now = datetime.now(timezone.utc)
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_lock(self, user_id):
        """Получает или создает блокировку для пользователя."""
        if user_id not in self.user_locks:
            self.user_locks[user_id] = asyncio.Lock()
        return self.user_locks[user_id]

    def check_limits(self, user_id, file_size):
        """Проверяет лимиты и сбрасывает их в 00:00 UTC."""
        now = datetime.now(timezone.utc)

        # Если наступил новый день (00:00 UTC), сбрасываем счетчики у всех
        if now > self.last_global_reset + timedelta(days=1):
            self.user_data.clear()  # Обнуляем данные всех пользователей
            self.last_global_reset = self._get_last_utc_midnight()

        # Инициализируем данные пользователя, если их нет
        if user_id not in self.user_data:
            self.user_data[user_id] = {'files_today': 0}

        # Проверяем лимиты
        if file_size > 15 * 1024 * 1024:  # 15 MB
            return False, "❌ Размер файла превышает 15 MB."

        if self.user_data[user_id]['files_today'] >= 10:
            time_left = (self.last_global_reset + timedelta(days=1)) - now
            hours_left = time_left.seconds // 3600
            minutes_left = (time_left.seconds % 3600) // 60
            return False, f"❌ Лимит исчерпан (10/10). Сброс через {hours_left} ч. {minutes_left} мин. (в 00:00 UTC)."

        return True, ""

    def increment_counter(self, user_id):
        """Увеличивает счетчик файлов пользователя."""
        if user_id not in self.user_data:
            self.user_data[user_id] = {'files_today': 1}
        else:
            self.user_data[user_id]['files_today'] += 1

# Создаем экземпляр класса лимитов
user_limits = UserLimits()

# Система очереди
class TaskQueue:
    def __init__(self, max_concurrent_tasks=5):
        self.queue = deque()  # Очередь задач
        self.active_tasks = {}  # Активные задачи: task_id -> task (вместо user_id -> task)
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_counter = 0  # Счетчик задач для назначения номера очереди
        self.user_tasks = {}  # Новый словарь: user_id -> [task_id1, task_id2, ...]

    def add_task(self, user_id, file_list, output_file_name):
        """Добавляет задачу в очередь и возвращает уникальный ID задачи и позицию в очереди"""
        self.task_counter += 1
        task_id = self.task_counter
        task = {
            'user_id': user_id,
            'file_list': file_list,
            'output_file_name': output_file_name,
            'task_id': task_id,
            'time_added': time.time()
        }
        self.queue.append(task)

        # Добавляем задачу в список задач пользователя
        if user_id not in self.user_tasks:
            self.user_tasks[user_id] = []
        self.user_tasks[user_id].append(task_id)

        return task_id, len(self.queue)

    def get_next_task(self):
        """Получить следующую задачу из очереди"""
        if not self.queue:
            return None
        task = self.queue.popleft()
        self.active_tasks[task['task_id']] = task  # Используем task_id вместо user_id
        return task

    def complete_task(self, task_id):
        """Пометить задачу как завершенную"""
        if task_id in self.active_tasks:
            task = self.active_tasks[task_id]
            user_id = task['user_id']

            # Удаляем задачу из active_tasks
            del self.active_tasks[task_id]

            # Удаляем задачу из списка задач пользователя
            if user_id in self.user_tasks and task_id in self.user_tasks[user_id]:
                self.user_tasks[user_id].remove(task_id)

                # Если у пользователя больше нет задач, удаляем его из словаря
                if not self.user_tasks[user_id]:
                    del self.user_tasks[user_id]

    def get_user_tasks(self, user_id):
        """Получить список всех задач пользователя (в очереди и активных)"""
        tasks = []

        # Ищем в очереди
        for task in self.queue:
            if task['user_id'] == user_id:
                tasks.append(task)

        # Ищем в активных задачах
        for task_id, task in self.active_tasks.items():
            if task['user_id'] == user_id:
                tasks.append(task)

        return tasks

    def can_process_now(self):
        """Проверка, можно ли обработать следующую задачу из очереди"""
        return len(self.active_tasks) < self.max_concurrent_tasks and self.queue

# Создаем очередь задач
task_queue = TaskQueue(max_concurrent_tasks=1)  # Максимум 5 одновременных задач

# Декоратор для измерения времени выполнения функции
def timer(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        elapsed = time.time() - start_time
        print(f"[PROFILING] Функция {func.__name__} выполнилась за {elapsed:.2f} секунд")
        return result
    return wrapper

# Замените токен на свой
API_TOKEN = '7692853253:AAHGfbJhall58TafIqTBdAujVnuXhhHCwYk'

bot = Bot(token=API_TOKEN)
router = Router()

# ===================== Неблокирующие функции конвертации =====================

# Функция-обертка для выполнения блокирующих операций в отдельном потоке
async def run_in_threadpool(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    func_partial = partial(func, *args, **kwargs)
    return await loop.run_in_executor(thread_pool, func_partial)

# Неблокирующие версии функций конвертации
async def convert_epub_to_docx(epub_file, docx_file):
    def _convert():
        # Открываем EPUB-файл
        book = epub.read_epub(epub_file)
        document = Document()
        # Перебираем элементы книги
        for item in book.get_items():
            if item.get_type() == ebooklib.ITEM_DOCUMENT:
                soup = BeautifulSoup(item.content, 'html.parser')
                for element in soup.find_all():
                    if element.name == 'h1':
                        document.add_heading(element.get_text(), level=0)
                    elif element.name == 'p':
                        doc_paragraph = document.add_paragraph()
                        # Перебор вложенных элементов абзаца
                        for sub in element.contents:
                            if hasattr(sub, 'name'):
                                if sub.name == 'strong':
                                    run = doc_paragraph.add_run(sub.get_text())
                                    run.bold = True
                                elif sub.name == 'em':
                                    run = doc_paragraph.add_run(sub.get_text())
                                    run.italic = True
                                else:
                                    doc_paragraph.add_run(sub.get_text())
                            else:
                                # Если это просто текст
                                doc_paragraph.add_run(sub)
        document.save(docx_file)

    return await run_in_threadpool(_convert)

async def convert_fb2_to_docx(fb2_file, docx_file):
    def _convert():
        with open(fb2_file, 'r', encoding='utf-8') as f:
            content = f.read()
        soup = BeautifulSoup(content, 'xml')
        document = Document()
        for element in soup.find_all():
            if element.name == 'title':
                document.add_heading(element.get_text(), level=0)
            elif element.name == 'p':
                # Если абзац не является частью title или annotation
                if element.find_parent(['title', 'annotation']) is None:
                    doc_paragraph = document.add_paragraph()
                    for sub in element.contents:
                        if hasattr(sub, 'name'):
                            if sub.name == 'strong':
                                run = doc_paragraph.add_run(sub.get_text())
                                run.bold = True
                            elif sub.name == 'emphasis':
                                run = doc_paragraph.add_run(sub.get_text())
                                run.italic = True
                            else:
                                doc_paragraph.add_run(sub.get_text())
                        else:
                            doc_paragraph.add_run(sub)
        document.save(docx_file)

    return await run_in_threadpool(_convert)

async def convert_txt_to_docx(txt_file, docx_file):
    def _convert():
        with open(txt_file, 'r', encoding='utf-8') as f:
            text = f.read()
        document = Document()
        for line in text.splitlines():
            document.add_paragraph(line)
        document.save(docx_file)

    return await run_in_threadpool(_convert)

@timer
async def process_files(file_list):
    """
    Обрабатывает список файлов, конвертируя их в формат .docx (если требуется)
    и возвращает список имен файлов в формате .docx для последующего объединения.
    """
    converted_files = []
    for file in file_list:
        ext = os.path.splitext(file)[1].lower()
        # Если файл уже в формате .docx – добавляем его в список
        if ext == ".docx":
            converted_files.append(file)
        elif ext == ".txt":
            docx_file = os.path.splitext(file)[0] + ".docx"
            await convert_txt_to_docx(file, docx_file)
            converted_files.append(docx_file)
        elif ext == ".fb2":
            docx_file = os.path.splitext(file)[0] + ".docx"
            await convert_fb2_to_docx(file, docx_file)
            converted_files.append(docx_file)
        elif ext == ".epub":
            docx_file = os.path.splitext(file)[0] + ".docx"
            await convert_epub_to_docx(file, docx_file)
            converted_files.append(docx_file)
    return converted_files

# ===================== Неблокирующие функции для работы с документами =====
def check_and_add_title(doc, file_name):
    """
    Проверяет первые абзацы документа на наличие заголовка (например, "Глава ...").
    Если заголовок не найден, добавляет его на основе имени файла.
    """
    patterns = [
        r'Глава[ ]{0,4}\d{1,4}',
        r'Часть[ ]{0,4}\d{1,4}',
        r'^Пролог[ .!]*$',
        r'^Описание[ .!]*$',
        r'^Аннотация[ .!]*$',
        r'^Annotation[ .!]*$',
        r'^Предисловие от автора[ .!]*$'
    ]
    if doc.paragraphs:
        check_paragraphs = doc.paragraphs[0:4]
        title_found = False
        for p in check_paragraphs:
            for pattern in patterns:
                if re.search(pattern, p.text):
                    title_found = True
                    break
            if title_found:
                break
        if not title_found:
            # Добавляем заголовок перед первым абзацем
            title = os.path.splitext(os.path.basename(file_name))[0]
            title_run = doc.paragraphs[0].insert_paragraph_before().add_run(f"{title}\n")
            # Форматирование заголовка
            title_run.bold = True
    return doc

@timer
async def merge_docx(file_list, output_file_name):
    def _merge():
         # Создаем новый документ
         merged_document = Document(file_list[0])
         merged_document = check_and_add_title(merged_document, file_list[0])
         composer = Composer(merged_document)
         for file in file_list[1:]:
             doc = Document(file)
             # Проверяем и добавляем название главы при необходимости
             doc = check_and_add_title(doc, file)
             composer.append(doc)
         # Сохраняем итоговый документ
         composer.save(output_file_name)
         print(f"Файлы объединены в {output_file_name}")
         return output_file_name

    # Объединяем обработанные файлы в отдельном потоке
    result = await run_in_threadpool(_merge)
    return result

# ===================== FSM: Состояния =====================
class MergeStates(StatesGroup):
    collecting = State()  # Состояние сбора файлов
    naming_file = State() # Состояние запроса имени файла

# ===================== Обработчики Telegram-бота =====================
@router.message(Command("start_merge"))
async def start_merge(message: Message, state: FSMContext):
    """
    Команда для начала сбора файлов.
    """
    current_state = await state.get_state()
    if current_state == MergeStates.collecting.state:
        await message.answer("Сбор файлов уже запущен.")
        return

    # Теперь мы не проверяем, есть ли у пользователя активная задача
    # Просто начинаем новый сбор файлов

    await state.set_state(MergeStates.collecting)
    await state.update_data(file_list=[])  # Создаем пустой список файлов
    await message.answer("Сбор файлов начат! Отправляйте файлы. Используйте /end_merge для завершения или /cancel для отмены.")

@router.message(Command("queue_status"))
async def queue_status(message: Message):
    """
    Проверка статуса очереди.
    """
    user_id = message.from_user.id
    user_tasks = task_queue.get_user_tasks(user_id)
    if not user_tasks:
        total_tasks = len(task_queue.queue)
        active_tasks = len(task_queue.active_tasks)
        await message.answer(f"У вас нет задач в очереди.\nСтатус системы: {active_tasks}/{task_queue.max_concurrent_tasks} активных задач, {total_tasks} задач в очереди.")
        return

    # Формируем сообщение со списком задач пользователя
    tasks_info = []
    for task in user_tasks:
        task_id = task['task_id']

        # Проверяем, активна ли задача
        if task_id in task_queue.active_tasks:
            status = "⚙️ Выполняется"
        else:
            # Ищем позицию в очереди
            for i, queued_task in enumerate(task_queue.queue):
                if queued_task['task_id'] == task_id:
                    status = f"🕒 В очереди (позиция {i+1})"
                    break

        # Создаем имя задачи из первого файла в списке
        task_name = os.path.basename(task['file_list'][0])
        if len(task['file_list']) > 1:
            task_name += f" и еще {len(task['file_list'])-1} файлов"

        tasks_info.append(f"Задача #{task_id}: {task_name} - {status}")
    await message.answer("Ваши задачи:\n\n" + "\n".join(tasks_info) + "\nВы можете отменить задачу с помощью /cancel_task")

@router.message(Command("cancel"))
async def cancel_collecting(message: Message, state: FSMContext):
    """
    Отмена сбора файлов.
    """
    current_state = await state.get_state()
    if current_state != MergeStates.collecting.state:
        await message.answer("Сбор файлов не был запущен.")
        return

    # Получаем список файлов, чтобы удалить их
    user_data = await state.get_data()
    file_list = user_data.get('file_list', [])

    # Удаляем временные файлы
    for file_item in file_list:
        file = file_item[0]
        if os.path.exists(file):
            os.remove(file)

    await state.clear()
    await message.answer("Сбор файлов отменен. Все временные файлы удалены.")

@router.message(Command("cancel_task"))
async def cancel_specific_task(message: Message):
    """
    Отмена конкретной задачи по ID.
    """
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Пожалуйста, укажите ID задачи: /cancel_task_id <task_id>")
        return

    try:
        task_id = int(args[1])
    except ValueError:
        await message.answer("ID задачи должен быть числом")
        return

    user_id = message.from_user.id

    # Ищем задачу в очереди
    found = False
    new_queue = deque()
    for task in task_queue.queue:
        if task['task_id'] == task_id:
            if task['user_id'] == user_id:
                found = True
                # Удаляем временные файлы
                for file in task['file_list']:
                    if os.path.exists(file):
                        os.remove(file)
            else:
                # Задача существует, но принадлежит другому пользователю
                await message.answer("Вы не можете отменить чужую задачу")
                return
        else:
            new_queue.append(task)

    if found:
        # Обновляем очередь
        task_queue.queue = new_queue

        # Удаляем task_id из списка задач пользователя
        if user_id in task_queue.user_tasks and task_id in task_queue.user_tasks[user_id]:
            task_queue.user_tasks[user_id].remove(task_id)

        await message.answer(f"Задача #{task_id} удалена из очереди")
    else:
        # Проверяем, не выполняется ли задача в данный момент
        if task_id in task_queue.active_tasks and task_queue.active_tasks[task_id]['user_id'] == user_id:
            await message.answer(f"Задача #{task_id} уже выполняется и не может быть отменена")
        else:
            await message.answer(f"Задача #{task_id} не найдена")

@router.message(Command("end_merge"))
async def end_merge(message: Message, state: FSMContext):
    """
    Команда для завершения сбора файлов и запроса имени выходного файла.
    """
    current_state = await state.get_state()
    if current_state != MergeStates.collecting.state:
        await message.answer("Сбор файлов не был запущен. Введите /start_merge для начала.")
        return

    user_data = await state.get_data()
    file_list = user_data.get('file_list', [])

    if not file_list:
        await message.answer("Нет файлов для обработки!")
        await state.clear()  # Очищаем состояние
        return

    # Переходим к состоянию запроса имени файла
    await state.set_state(MergeStates.naming_file)

    # Создаем клавиатуру с кнопкой "Пропустить"
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(types.KeyboardButton(text="Пропустить"))
    keyboard.adjust(1)

    await message.answer(
        "Введите название для итогового файла или нажмите 'Пропустить' для использования стандартного имени (merged.docx):",
        reply_markup=keyboard.as_markup(resize_keyboard=True)
    )

@router.message(MergeStates.naming_file)
async def process_filename(message: Message, state: FSMContext):
    """
    Обработка введенного имени файла или кнопки "Пропустить".
    """
    user_id = message.from_user.id
    user_data = await state.get_data()
    file_list = user_data.get('file_list', [])

    # Сортируем файлы по ID сообщения (второй элемент кортежа)
    file_list.sort(key=lambda x: x[1])

    # Извлекаем только имена файлов после сортировки
    sorted_files = [file[0] for file in file_list]

    # Определяем имя выходного файла
    if message.text == "Пропустить":
        output_file_name = "merged.docx"
    else:
        output_file_name = message.text + ".docx"

    # Добавляем задачу в очередь с отсортированным списком файлов
    task_id, queue_position = task_queue.add_task(user_id, sorted_files, output_file_name)

    # Возвращаем обычную клавиатуру
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(types.KeyboardButton(text="/start_merge"))
    keyboard.add(types.KeyboardButton(text="/end_merge"))
    keyboard.add(types.KeyboardButton(text="/cancel"))
    keyboard.add(types.KeyboardButton(text="/queue_status"))
    keyboard.adjust(2)

    if queue_position > 1:
        await message.answer(
            f"Итоговый файл будет назван: {output_file_name}\n"
            f"Ваша задача добавлена в очередь на позицию {queue_position}. "
            f"Примерное время ожидания: {queue_position * 2} минут(ы). "
            f"Используйте /queue_status для проверки статуса.",
            reply_markup=keyboard.as_markup(resize_keyboard=True)
        )
    else:
        await message.answer(
            f"Итоговый файл будет назван: {output_file_name}\n"
            f"Ваша задача добавлена в очередь и будет обработана в ближайшее время.",
            reply_markup=keyboard.as_markup(resize_keyboard=True)
        )

    # Очищаем состояние после добавления задачи в очередь
    await state.clear()

    # Пытаемся запустить обработку задачи, если есть свободные потоки
    asyncio.create_task(check_and_process_queue())

async def check_and_process_queue():
    """
    Проверяет очередь и запускает обработку новых задач, если есть свободные ресурсы.
    """
    while task_queue.can_process_now():
        task = task_queue.get_next_task()
        if task:
            user_id = task['user_id']
            file_list = task['file_list']
            output_file_name = task['output_file_name']
            task_id = task['task_id']

            # Уведомляем пользователя о начале обработки
            await bot.send_message(user_id, f"Начинаю обработку задачи #{task_id} с {len(file_list)} файлами. Это может занять некоторое время...")

            # Запускаем обработку в фоновом режиме
            asyncio.create_task(process_and_merge_files_with_queue(user_id, file_list, output_file_name, task_id))

async def process_and_merge_files_with_queue(user_id, file_list, output_file_name, task_id):
    """
    Асинхронная функция для обработки и объединения файлов с учетом очереди.
    """
    try:
        # Обработка и конвертация файлов
        converted_files = await process_files(file_list)
        merged_file = await merge_docx(converted_files, output_file_name)

        # Формируем сообщение с информацией о собранных файлах
        file_list_str = "\n".join([os.path.basename(f) for f in file_list])
        await bot.send_message(user_id, f"Задача #{task_id} завершена!\nФайлы объединены в {os.path.basename(output_file_name)}.\nСобрано {len(file_list)} файлов:\n{file_list_str}")

        # Отправляем объединённый файл обратно пользователю
        document = FSInputFile(merged_file)
        await bot.send_document(user_id, document, caption=f"Результат задачи #{task_id}")

        # Удаляем временные файлы
        for file in file_list:
            if os.path.exists(file):
                os.remove(file)

        # Удаляем объединенный файл после отправки
        if os.path.exists(merged_file):
            os.remove(merged_file)

    except Exception as e:
        await bot.send_message(user_id, f"Произошла ошибка при обработке задачи #{task_id}: {str(e)}")
    finally:
        # Отмечаем задачу как выполненную
        task_queue.complete_task(task_id)  # Теперь передаем task_id вместо user_id

        # Проверяем, можно ли обработать следующую задачу
        asyncio.create_task(check_and_process_queue())

@router.message(F.document)
async def handle_document(message: Message, state: FSMContext):
    """
    Обработчик полученных файлов.
    Если сбор файлов запущен, сохраняет полученный документ на диск
    и добавляет его имя в список для дальнейшей обработки.
    """
    current_state = await state.get_state()
    if current_state != MergeStates.collecting.state:
        await message.answer("Сбор файлов не запущен. Введите /start_merge для начала.")
        return

    user_id = message.from_user.id
    file_size = message.document.file_size
    lock = user_limits.get_lock(user_id) # Получаем блокировку пользователя

    async with lock: # Захватываем блокировку (освободится автоматически при выходе из блока)
        # --- Начало критической секции ---
        is_allowed, error_msg = user_limits.check_limits(user_id, file_size)
        if not is_allowed:
            await message.answer(error_msg)
            return # Выходим, блокировка освобождается

        # Если лимит позволяет, СРАЗУ увеличиваем счетчик ВНУТРИ блокировки
        user_limits.increment_counter(user_id)
        files_today_count = user_limits.user_data[user_id]['files_today']
        files_left = 10 - files_today_count
        # --- Конец критической секции ---

    # --- Операции вне блокировки (загрузка, сохранение) ---
    try:
        file_info = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        file_name = message.document.file_name

        # Добавляем user_id к имени файла, чтобы избежать конфликтов между пользователями
        base_name, extension = os.path.splitext(file_name)
        counter = 1

        if extension.lower() not in (".docx", ".fb2", ".txt", ".epub"):
            await message.answer(f"Неизвестный формат файла: {message.document.file_name}. Пожалуйста, отправляйте файлы только в форматах docx, fb2, epub, txt.")
            return

        while os.path.exists(file_name):
            file_name = f"{base_name}({counter}){extension}"
            counter += 1

        # Сохраняем файл на диск
        async with aiofiles.open(file_name, 'wb') as new_file:
            await new_file.write(downloaded_file.read())

        # Добавляем файл в список вместе с ID сообщения
        user_data = await state.get_data()
        file_list = user_data.get('file_list', [])
        # Теперь храним кортеж (имя_файла, id_сообщения)
        file_list.append((file_name, message.message_id))
        await state.update_data(file_list=file_list)

        # Сообщаем о лимитах
        await message.answer(
            f"Файл {file_name} сохранён! Всего файлов: {len(file_list)}\n"
            f"Использовано сегодня: {files_today_count}/10" # Показываем актуальное число
        )
    except Exception as e:
        await message.answer(f"Ошибка при сохранении файла: {str(e)}")

@router.message(Command("start"))
async def send_welcome(message: Message):
    await message.answer("Привет, я бот для объединения файлов! Нажми /info для получения дополнительной информации.")

@router.message(Command("info"))
async def send_info(message: Message):
    keyboard = ReplyKeyboardBuilder()
    keyboard.add(types.KeyboardButton(text="/start_merge"))
    keyboard.add(types.KeyboardButton(text="/end_merge"))
    keyboard.add(types.KeyboardButton(text="/cancel"))
    keyboard.add(types.KeyboardButton(text="/queue_status"))
    keyboard.adjust(2)

    await message.answer(
        "📚 Бот для объединения файлов (DOCX, FB2, EPUB, TXT).\n\n"
        "Лимиты:\n"
        "• 10 файлов в сутки (сброс в 00:00 UTC)\n"
        "• Макс. размер файла: 15 MB\n\n"
        "Команды:\n"
        "/start_merge – начать сбор файлов\n"
        "/end_merge – завершить и объединить\n"
        "/limits – проверить лимиты\n"
        "/queue_status – статус очереди\n"
        "/cancel – отменить текущий сбор",
        reply_markup=keyboard.as_markup(resize_keyboard=True)
    )

@router.message(Command("limits"))
async def check_limits(message: Message):
    """Показывает текущие лимиты и время до сброса."""
    user_id = message.from_user.id
    now = datetime.now(timezone.utc)
    next_reset = user_limits.last_global_reset + timedelta(days=1)
    time_left = next_reset - now

    hours_left = time_left.seconds // 3600
    minutes_left = (time_left.seconds % 3600) // 60

    if user_id not in user_limits.user_data:
        files_used = 0
    else:
        files_used = user_limits.user_data[user_id]['files_today']
    files_left = 10 - files_used

    await message.answer(
        f"📊 Ваши лимиты:\n"
        f"• Использовано файлов: {files_used}/10\n"
        f"• Осталось файлов: {files_left}\n"
        f"• Максимальный размер файла: 15 MB\n"
        f"Лимит сбросится в 00:00 UTC (через {hours_left} ч. {minutes_left} мин.)"
    )

# ===================== Запуск бота =====================
async def main():
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    print("Бот запущен.")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
