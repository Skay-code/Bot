@router.message(Command("queue_status"))
async def queue_status(message: Message):
    """
    Проверка статуса очереди.
    """
    if await check_sender(message):
        return

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

@router.message(Command("cancel_task"))
async def cancel_specific_task(message: Message):
    """
    Отмена конкретной задачи по ID.
    """
    if await check_sender(message):
        return

    args = message.text.split()
    print(args)
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
        await message.answer(f"Задача #{task_id} удалена из очереди")
    else:
        # Проверяем, не выполняется ли задача в данный момент
        if task_id in task_queue.active_tasks and task_queue.active_tasks[task_id]['user_id'] == user_id:
            await message.answer(f"Задача #{task_id} уже выполняется и не может быть отменена")
        else:
            await message.answer(f"Задача #{task_id} не найдена")
