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
            if any(p.style.name.lower().startswith(prefix) for prefix in ["heading", "заголовок"]):
                title_found = True
                break

        if not title_found:
            for p in check_paragraphs:
                for pattern in patterns:
                    if re.search(pattern, p.text.strip()):
                        title_found = True
                        break
                if title_found:
                    break
        if not title_found:
            # Добавляем заголовок перед первым абзацем
            style_names = ['Heading 1', 'Заголовок 1']
            title = os.path.splitext(os.path.basename(file_name))[0]
            paragraph = doc.paragraphs[0].insert_paragraph_before(title)
            for name in style_names:
                try:
                    paragraph.style = name
                    break
                except Exception as e:
                    print(f"Стиль {name} не получилось установить: {e}")
    return doc

@timer
async def merge_docx(file_list, output_file_name):
    def _merge():
        # Создаем новый документ
        merged_document = Document()
        composer = Composer(merged_document)
        try:
            for file in file_list:
                try:
                    doc = Document(file)
                    doc = check_and_add_title(doc, file)
                    composer.append(doc)
                except Exception as e:
                    print(f"Ошибка добавления файла {file}: {e}")
                    merged_document.add_paragraph(f"Ошибка добавления файла {os.path.basename(file)}: {e}")
        except Exception as e:
            print(f"Критическая ошибка, невозможно пройтись по списку {file_list}: {e}")
            merged_document.add_paragraph(f"Критическая ошибка, невозможно пройтись по списку {file_list}: {e}")
        finally:
            composer.save(output_file_name)
            print(f"Файлы объединены в {output_file_name}")
            return output_file_name

    # Объединяем обработанные файлы в отдельном потоке
    result = await run_in_threadpool(_merge)
    return result
