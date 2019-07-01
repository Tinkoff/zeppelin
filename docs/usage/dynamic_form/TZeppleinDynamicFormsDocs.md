# TZepplein Dynamic Forms Docs
Example:
```
%FORM
  #define form_name1 type<TextBox>    title<Title_text>
  #define form_name2 type<Password>   title<Title_text>
  #define form_name3 type<Select>     title<Title_text> options<option1;option2;option3> autorun<false>
  #define form_name4 type<CheckBox>   title<Title_text> options<option1;option2;option3> autorun<false>
  #define form_name5 type<DatePicker> title<Title_text> pattern<dd-mm-yyyy hh:ii>
%FORM
```

Формы вставляются в текст параграфа (интерпретатором игнорируются).  
Для использования в код вставляется конструкция вида `#имя_формы#`.  
При выполнении конструкция будет заменена на значение формы.  

### Описание параметров форм:
| Параметр                 | Варианты                                        | Обязательное          | Описание                                                                                                   |    
|--------------------------|-------------------------------------------------|:---------------------:|------------------------------------------------------------------------------------------------------------|
|#define `Имя_формы`       | Any String                                      | Да                    | Имя формы (используется в тексте параграфа)                                                                |
|type<`Тип_Формы`>         | TextBox, Password, Select, CheckBox, DatePicker | Да                    | Тип формы                                                                                                  |
|title<`Title_text`>       | Any String                                      | Нет                   | Заголов формы                                                                                              |
|options<`opt1;...;optN`>  | Any String; Any String;...;                     | Для Select и CheckBox | Список параметров для Select и CheckBox форм                                                               |
|autorun<`false\true`>     | `false` or `true`                               | Нет                   | Автозапуск параграфа после изменения значения формы. По умолчанию true для всех кроме(TextBox и Password)  |
|pattern<`DatePattern`>    | См. описания паттерна ниже                      | Нет                   | Паттерн строки получаемой на выходе. По умолчанию `dd.mm.yyy`                                              |


### DatePicker Паттерн
examples:
```
pattern<dd-mm-yyyy hh:ii>
pattern<dd-mm-yyyy>
pattern<hh:ii>
```
Для включения 12-ти часового режима добавьте 'aa' или 'AA'  
#### Date
| Date Pattern  | Описание                                           | Time Pattern | Описание                      |
|---------------|----------------------------------------------------|--------------|-------------------------------|
|@              | время в миллесекундах                              |h             | часы                          |
|d              | дата                                               |hh            | часы, с лидирующим нулем      |
|dd             | дата с лидирующем нулем                            |i             | минуты                        |
|D              | сокращенное наименование дня                       |ii            | минуты, с лидирующим нулем    |
|DD             | полное наименование дня                            |aa            | период дня - 'am' или 'pm'    |
|m              | номер мясяца                                       |AA            | период дня заглавными буквами | 
|mm             | номер месяца с лидирующем нулем                    |     -        |              -                |
|M              | сокращенное наименовение месяца                    |     -        |              -                |
|MM             | полное наименовение месяца                         |     -        |              -                |
|yy             | сокращенный номер года                             |     -        |              -                |
|yyyy           | полный номер года                                  |     -        |              -                |
|yyyy1          | первый год декады, в которую входит текущий год    |     -        |              -                |
|yyyy2          | последний год декады, в которую входит текущий год |     -        |              -                |
