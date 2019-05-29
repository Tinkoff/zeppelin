# Note Teleport
1. Создать директорию для работы со старыми ноутами (далее `.../workDir`).
2. Создать в нём директорию `notes` (т.е. получится`.../workDir/notes`).
3. Скопировать ноуты в директорию `.../workDir/notes`.
4. Создать файл для миграции старых имён групп на новые `.../workDir/role-association.txt` (Файл есть у a.savelenko)  
пример наполнения `oldGroupName : newGroupName`
5. Создать каталог `../workDir/notebook-authorizations`. 
Поместить в него файлы с описанием прав ноутов. (`notebook-authorization.json`)
Имена файлов значения не имеет.
6. Запустить миграцию `java -jar NotesTeleport.jar полный_путь_к_workDir адрес_zeppelin`

### Примечания:
1. Запросы на rest идут без авторизации так что включаем anonymous на время миграции
2. Во время запуска у пользователя будут запрашиваться ассоциации для старого шебанга  
(Они будут храниться в `.../workDir/shebang-association.json`)
3. Ноуты успешно прошедшие миграции попадут в папку `.../workDir/processed`
4. Ноуты с ошибками при парсинге json файла ноута попадут в `.../workDir/defective/load`
5. Ноуты с ошибками во время отправки rest запроса попадут в `.../workDir/defective/persist`
6. Чтобы отключить все cron таски дописать параметр `disableCron` при запуске.  
Т.е. должно получится `java -jar NotesTeleport.jar полный_путь_к_workDir адрес_zeppelin disableCron` 
7. Для заполнения пустых полей прав (Owners/Writers/Runners/Readers) в файле `role-association.txt` нужно задать 
группы:  
```
owner_null       : zeppelin-owner
writer_null      : zeppelin-writer
runner_null      : zeppelin-runner
reader_null      : zeppelin-reader

```
